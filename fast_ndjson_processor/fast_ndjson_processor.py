import io
import logging
import multiprocessing as mp
from functools import partial
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Protocol,
    Tuple,
    Union,
    runtime_checkable,
    NamedTuple,
)

import orjson

from .exceptions import (
    ConfigurationError,
    FileHandlingError,
    ProcessingError,
    SerializationError,
)
from .processor_mode import ProcessorMode

logger = logging.getLogger(__name__)


class BatchInfo(NamedTuple):
    """Information about a batch being processed."""
    batch_id: int
    batch_start_line: int
    batch_end_line: int
    batch_size: int


@runtime_checkable
class JSONRecord(Protocol):
    """Protocol for JSON record types."""

    pass


RecordType = Dict[str, Any]
ChunkType = List[RecordType]
HandlerReturn = Any
ChunkHandlerReturn = Any

from tqdm import tqdm


class FastNDJSONProcessor:
    """
    A high-performance NDJSON file processor with multiprocessing support.

    Examples:
        Basic usage with default settings:
        >>> processor = FastNDJSONProcessor()
        >>> results = processor.process_file("data.ndjson", lambda x: x["id"])

        Custom processing with configuration:
        >>> processor = FastNDJSONProcessor(n_workers=8, chunk_size=1000000)
        >>> def custom_handler(record):
        ...     return {"id": record["id"], "processed": True}
        >>> results = processor.process_file("data.ndjson", custom_handler)

        Process specific number of batches with batch information:
        >>> processor = FastNDJSONProcessor(n_batches=64)
        >>> def batch_handler(records, batch_info):
        ...     return {
        ...         "batch_id": batch_info.batch_id,
        ...         "count": len(records),
        ...         "lines": f"{batch_info.batch_start_line}-{batch_info.batch_end_line}"
        ...     }
        >>> results = processor.process_file_chunks("data.ndjson", batch_handler, include_batch_info=True)

        Process specific line range:
        >>> processor = FastNDJSONProcessor(start_line=100, end_line=199)
        >>> results = processor.process_file("data.ndjson", lambda x: x["id"])

        Streaming mode for memory efficiency:
        >>> for record in processor.stream_file("large.ndjson"):
        ...     process_record(record)
    """

    def __init__(
        self,
        n_workers: Optional[int] = None,
        chunk_size: Optional[int] = None,
        encoding: str = "utf-8",
        skip_errors: bool = True,
        show_progress: bool = False,
        n_batches: Optional[int] = None,
        start_line: Optional[int] = None,
        end_line: Optional[int] = None,
    ) -> None:
        """
        Initialize the NDJSON processor.

        Args:
            n_workers: Number of worker processes (defaults to CPU count)
            chunk_size: Size of chunks in bytes (auto-calculated if None)
            encoding: File encoding (default: 'utf-8')
            skip_errors: Whether to skip malformed JSON lines (default: True)
            show_progress: Show progress bar if tqdm is available (default: False)
            n_batches: Number of batches to create (overrides chunk_size calculation)
            start_line: First line to process (1-based, defaults to 1)
            end_line: Last line to process (1-based, defaults to end of file)

        Raises:
            ValueError: If n_workers, chunk_size, n_batches are negative or if line ranges are invalid
        """
        if n_workers is not None and n_workers <= 0:
            raise ConfigurationError("n_workers must be positive")
        if chunk_size is not None and chunk_size <= 0:
            raise ConfigurationError("chunk_size must be positive")
        if n_batches is not None and n_batches <= 0:
            raise ConfigurationError("n_batches must be positive")
        if start_line is not None and start_line < 1:
            raise ConfigurationError("start_line must be >= 1")
        if end_line is not None and end_line < 1:
            raise ConfigurationError("end_line must be >= 1")
        if (start_line is not None and end_line is not None and start_line > end_line):
            raise ConfigurationError("start_line must be <= end_line")

        self.n_workers = n_workers or mp.cpu_count()
        self.chunk_size = chunk_size
        self.encoding = encoding
        self.skip_errors = skip_errors
        self.show_progress = show_progress
        self.n_batches = n_batches
        self.start_line = start_line
        self.end_line = end_line

    def process_file(
        self,
        filepath: Union[str, Path],
        handler: Callable[[RecordType], HandlerReturn],
        mode: ProcessorMode = ProcessorMode.PARALLEL,
        progress_desc: str = "Processing chunks",
        return_results: bool = True,
    ) -> Optional[List[HandlerReturn]]:
        """
        Process an NDJSON file with the specified handler function.

        Args:
            filepath: Path to the NDJSON file
            handler: Function to process each JSON record
            mode: Processing mode (PARALLEL, SEQUENTIAL, or STREAMING)
            return_results: Whether to return results (default: True)
            progress_desc: Description for progress bar (default: 'Processing chunks')

        Returns:
            List of processed results if return_results=True, None otherwise

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If an invalid processing mode is specified
        """
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileHandlingError(f"File not found: {filepath}")
        if not filepath.is_file():
            raise FileHandlingError(f"Path is not a file: {filepath}")

        if mode == ProcessorMode.PARALLEL:
            results = self._process_parallel(
                filepath, handler, return_results, progress_desc, chunk_handler=False
            )
        elif mode == ProcessorMode.SEQUENTIAL:
            results = self._process_sequential(
                filepath, handler, return_results, progress_desc, chunk_handler=False
            )
        elif mode == ProcessorMode.STREAMING:
            results = list(self.stream_file(filepath, handler)) if return_results else None
        else:
            raise ConfigurationError(
                f"Invalid processing mode: {mode}. Must be one of {list(ProcessorMode)}"
            )

        return results

    def process_file_chunks(
        self,
        filepath: Union[str, Path],
        chunk_handler: Union[
            Callable[[ChunkType], ChunkHandlerReturn],
            Callable[[ChunkType, BatchInfo], ChunkHandlerReturn]
        ],
        mode: ProcessorMode = ProcessorMode.PARALLEL,
        progress_desc: str = "Processing chunks",
        return_results: bool = True,
        include_batch_info: bool = False,
    ) -> Optional[List[ChunkHandlerReturn]]:
        """
        Process an NDJSON file with chunk-level processing.

        Each worker receives a list of records (chunk) to process at once,
        allowing for custom batch operations, aggregations, or transformations.

        Args:
            filepath: Path to the NDJSON file
            chunk_handler: Function to process each chunk. If include_batch_info=True,
                          function should accept (records, batch_info) parameters.
            mode: Processing mode (PARALLEL or SEQUENTIAL, STREAMING not supported)
            progress_desc: Description for progress bar
            return_results: Whether to return results
            include_batch_info: If True, passes BatchInfo as second parameter to chunk_handler

        Returns:
            List of chunk processing results if return_results=True, None otherwise

        Examples:
            Basic chunk processing:
            >>> def analyze_chunk(records):
            ...     return {
            ...         "count": len(records),
            ...         "avg_value": sum(r.get("value", 0) for r in records) / len(records),
            ...         "unique_ids": list(set(r["id"] for r in records))
            ...     }
            >>> processor = FastNDJSONProcessor()
            >>> results = processor.process_file_chunks("data.ndjson", analyze_chunk)

            With batch information:
            >>> def analyze_with_info(records, batch_info):
            ...     return {
            ...         "batch_id": batch_info.batch_id,
            ...         "lines": f"{batch_info.batch_start_line}-{batch_info.batch_end_line}",
            ...         "count": len(records),
            ...         "batch_size": batch_info.batch_size
            ...     }
            >>> processor = FastNDJSONProcessor(n_batches=8)
            >>> results = processor.process_file_chunks("data.ndjson", analyze_with_info, include_batch_info=True)

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If an invalid processing mode is specified
        """
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileHandlingError(f"File not found: {filepath}")
        if not filepath.is_file():
            raise FileHandlingError(f"Path is not a file: {filepath}")

        if mode == ProcessorMode.PARALLEL:
            results = self._process_parallel(
                filepath, chunk_handler, return_results, progress_desc, chunk_handler=True, include_batch_info=include_batch_info
            )
        elif mode == ProcessorMode.SEQUENTIAL:
            results = self._process_sequential(
                filepath, chunk_handler, return_results, progress_desc, chunk_handler=True, include_batch_info=include_batch_info
            )
        elif mode == ProcessorMode.STREAMING:
            raise ConfigurationError("Chunk processing is not supported in STREAMING mode")
        else:
            raise ConfigurationError(
                f"Invalid processing mode: {mode}. Must be one of {list(ProcessorMode)}"
            )

        return results

    def stream_file(
        self,
        filepath: Union[str, Path],
        handler: Optional[Callable[[RecordType], HandlerReturn]] = None,
    ) -> Iterator[Union[RecordType, HandlerReturn]]:
        """
        Stream process an NDJSON file line by line.

        Args:
            filepath: Path to the NDJSON file
            handler: Optional function to process each record

        Yields:
            Processed records if handler provided, otherwise raw JSON objects

        Raises:
            FileNotFoundError: If the file doesn't exist
            orjson.JSONDecodeError: If skip_errors=False and invalid JSON is encountered
        """
        filepath = Path(filepath)

        if not filepath.exists():
            raise FileHandlingError(f"File not found: {filepath}")
        if not filepath.is_file():
            raise FileHandlingError(f"Path is not a file: {filepath}")

        try:
            with open(filepath, "r", encoding=self.encoding) as f:
                line_number = 0
                for line in f:
                    line_number += 1
                    try:
                        parsed = orjson.loads(line.strip())
                        result = parsed if handler is None else handler(parsed)
                        yield result
                    except orjson.JSONDecodeError as e:
                        if not self.skip_errors:
                            raise SerializationError(
                                f"Invalid JSON on line {line_number}: {e}", original_error=e
                            )
                        logger.warning(f"Skipped invalid JSON on line {line_number}: {e}")
                    except Exception as e:
                        if not self.skip_errors:
                            raise ProcessingError(
                                f"Handler failed on line {line_number}: {e}",
                                line_number=line_number,
                                original_error=e,
                            )
                        logger.warning(f"Handler failed on line {line_number}: {e}")
        except (OSError, IOError) as e:
            raise FileHandlingError(f"Failed to read file {filepath}: {e}") from e

    def _process_sequential(
        self,
        filepath: Path,
        handler: Union[
            Callable[[RecordType], HandlerReturn], Callable[[ChunkType], ChunkHandlerReturn]
        ],
        return_results: bool = True,
        progress_desc: str = "Processing records",
        chunk_handler: bool = False,
        include_batch_info: bool = False,
    ) -> Optional[Union[List[HandlerReturn], List[ChunkHandlerReturn]]]:
        """Process file sequentially in a single thread."""
        results = [] if return_results else None

        if not filepath.exists():
            raise FileHandlingError(f"File not found: {filepath}")
        if not filepath.is_file():
            raise FileHandlingError(f"Path is not a file: {filepath}")

        if chunk_handler:
            # Use line-based chunk processing
            chunks = self._calculate_chunks(filepath)

            if not chunks:
                logger.warning(f"No chunks to process for file {filepath}")
                return results

            # Set up progress bar if needed
            chunk_iter = chunks
            if self.show_progress:
                chunk_iter = tqdm(chunks, desc=progress_desc)

            for chunk_id, chunk_start_line, chunk_end_line, _, _ in chunk_iter:
                # Always create batch_info for line ranges, but only pass it to handler if requested
                batch_info = BatchInfo(
                    batch_id=chunk_id,
                    batch_start_line=chunk_start_line,
                    batch_end_line=chunk_end_line,
                    batch_size=chunk_end_line - chunk_start_line + 1,
                )

                result = _process_chunk_with_handler(
                    chunk=(0, 0),  # Not used for line-based processing
                    filepath=str(filepath),
                    chunk_handler=handler,
                    encoding=self.encoding,
                    skip_errors=self.skip_errors,
                    return_results=return_results,
                    include_batch_info=include_batch_info,
                    batch_info=batch_info,  # Always pass batch_info for line ranges
                )
                if return_results and result is not None:
                    results.append(result)
        else:
            # Record-by-record processing using line-based chunks
            chunks = self._calculate_chunks(filepath)
            if not chunks:
                logger.warning(f"No chunks to process for file {filepath}")
                return results

            # Set up progress bar if needed
            chunk_iter = chunks
            if self.show_progress:
                chunk_iter = tqdm(chunks, desc=progress_desc)

            for chunk_id, chunk_start_line, chunk_end_line, _, _ in chunk_iter:
                result = _process_chunk(
                    chunk=(chunk_start_line, chunk_end_line),
                    filepath=str(filepath),
                    handler=handler,
                    encoding=self.encoding,
                    skip_errors=self.skip_errors,
                    return_results=return_results,
                )
                if return_results and result is not None:
                    results.extend(result)

        return results

    def _process_parallel(
        self,
        filepath: Path,
        handler: Union[
            Callable[[RecordType], HandlerReturn], Callable[[ChunkType], ChunkHandlerReturn]
        ],
        return_results: bool = True,
        progress_desc: str = "Processing chunks",
        chunk_handler: bool = False,
        include_batch_info: bool = False,
    ) -> Optional[Union[List[HandlerReturn], List[ChunkHandlerReturn]]]:
        """Process file in parallel using multiple processes."""
        all_results = [] if return_results else None

        if chunk_handler:
            # Use line-based chunk processing
            chunks = self._calculate_chunks(filepath)

            # Validate chunks before starting multiprocessing
            if not chunks:
                logger.warning(f"No chunks to process for file {filepath}")
                return all_results

            with mp.Pool(processes=self.n_workers) as pool:
                worker_args = []

                for chunk_id, chunk_start_line, chunk_end_line, _, _ in chunks:
                    # Always create batch_info for line ranges
                    batch_info = BatchInfo(
                        batch_id=chunk_id,
                        batch_start_line=chunk_start_line,
                        batch_end_line=chunk_end_line,
                        batch_size=chunk_end_line - chunk_start_line + 1,
                    )

                    args = (
                        (0, 0),  # Not used for line-based processing
                        str(filepath),
                        handler,
                        self.encoding,
                        self.skip_errors,
                        return_results,
                        include_batch_info,
                        batch_info,
                    )
                    worker_args.append(args)

                # Use starmap for processing
                result_iter = pool.starmap(_process_chunk_with_handler, worker_args)

                # Process results as they arrive to minimize memory usage
                if self.show_progress:
                    result_iter = tqdm(result_iter, total=len(worker_args), desc=progress_desc)

                # Process results as they come in
                for result in result_iter:
                    if return_results and result is not None:
                        all_results.append(result)
        else:
            # Record-by-record processing using line-based chunks
            chunks = self._calculate_chunks(filepath)
            if not chunks:
                logger.warning(f"No chunks to process for file {filepath}")
                return all_results

            with mp.Pool(processes=self.n_workers) as pool:
                worker_args = []

                for chunk_id, chunk_start_line, chunk_end_line, _, _ in chunks:
                    args = (
                        (chunk_start_line, chunk_end_line),  # Pass line range for record processing
                        str(filepath),
                        handler,
                        self.encoding,
                        self.skip_errors,
                        return_results,
                    )
                    worker_args.append(args)

                # Use starmap for processing
                result_iter = pool.starmap(_process_chunk, worker_args)

                # Process results as they arrive to minimize memory usage
                if self.show_progress:
                    result_iter = tqdm(result_iter, total=len(worker_args), desc=progress_desc)

                # Process results as they come in
                for result in result_iter:
                    if return_results and result is not None:
                        all_results.extend(result)  # Record handlers return lists

        return all_results

    def _count_lines(self, filepath: Path) -> int:
        """Count the total number of lines in the file."""
        try:
            with open(filepath, "r", encoding=self.encoding) as f:
                return sum(1 for _ in f)
        except (OSError, IOError) as e:
            raise FileHandlingError(f"Failed to count lines in file {filepath}: {e}") from e

    def _calculate_chunks(self, filepath: Path) -> List[Tuple[int, int, int, int, int]]:
        """Calculate line-based chunks for processing."""
        # Always use line-based batching for simplicity and correctness
        total_lines = self._count_lines(filepath)

        if total_lines == 0:
            return []

        # Determine line range to process
        actual_start_line = self.start_line or 1
        actual_end_line = self.end_line or total_lines

        # Validate line range
        if actual_start_line > total_lines:
            actual_start_line = total_lines
        if actual_end_line > total_lines:
            actual_end_line = total_lines

        lines_to_process = actual_end_line - actual_start_line + 1

        # Determine number of chunks
        if self.n_batches:
            num_chunks = min(self.n_batches, lines_to_process)
        else:
            num_chunks = min(self.n_workers, lines_to_process)

        # Calculate lines per chunk
        lines_per_chunk = lines_to_process // num_chunks
        extra_lines = lines_to_process % num_chunks

        chunks = []
        current_line = actual_start_line

        for chunk_id in range(num_chunks):
            chunk_lines = lines_per_chunk + (1 if chunk_id < extra_lines else 0)
            chunk_start_line = current_line
            chunk_end_line = current_line + chunk_lines - 1

            chunks.append((chunk_id, chunk_start_line, chunk_end_line, 0, 0))
            current_line = chunk_end_line + 1

        return chunks


def _process_chunk(
    chunk: Tuple[int, int],
    filepath: str,
    handler: Callable[[RecordType], HandlerReturn],
    encoding: str,
    skip_errors: bool,
    return_results: bool = True,
) -> Optional[List[HandlerReturn]]:
    """Worker function to process a line range of the file."""
    start_line, end_line = chunk
    results = [] if return_results else None

    try:
        with open(filepath, "r", encoding=encoding) as f:
            current_line = 1

            # Skip to start line
            for _ in range(start_line - 1):
                line = f.readline()
                if not line:  # EOF
                    break
                current_line += 1

            # Process lines in this range
            while current_line <= end_line:
                line = f.readline()
                if not line:  # EOF
                    break

                try:
                    record = orjson.loads(line.strip())
                    if handler is not None:
                        result = handler(record)
                    else:
                        result = None
                    if return_results and result is not None:
                        results.append(result)
                except orjson.JSONDecodeError as e:
                    if not skip_errors:
                        raise SerializationError(
                            f"Invalid JSON on line {current_line}: {e}",
                            original_error=e,
                        )
                    # Skip silently in multiprocessing to avoid logging issues
                except Exception as e:
                    if not skip_errors:
                        raise ProcessingError(
                            f"Handler failed on line {current_line}: {e}",
                            original_error=e,
                        )
                    # Skip silently in multiprocessing to avoid logging issues

                current_line += 1

    except (OSError, IOError) as e:
        raise FileHandlingError(f"Failed to process chunk in file {filepath}: {e}") from e

    if return_results:
        return results
    else:
        return None


def _process_chunk_with_handler(
    chunk: Tuple[int, int],
    filepath: str,
    chunk_handler: Union[
        Callable[[ChunkType], ChunkHandlerReturn],
        Callable[[ChunkType, BatchInfo], ChunkHandlerReturn]
    ],
    encoding: str,
    skip_errors: bool,
    return_results: bool = True,
    include_batch_info: bool = False,
    batch_info: Optional[BatchInfo] = None,
) -> Optional[ChunkHandlerReturn]:
    """Worker function to process a chunk with a chunk-level handler."""
    start, end = chunk
    records = []

    try:
        # Always use line-based processing now - much simpler!
        if batch_info is not None:
            start_line = batch_info.batch_start_line
            end_line = batch_info.batch_end_line
        else:
            # For backward compatibility, process all lines from beginning to end
            # This is a fallback that shouldn't normally be used
            start_line = 1
            end_line = None  # Process until EOF

        with open(filepath, "r", encoding=encoding) as f:
            current_line = 1

            # Skip to start line
            for _ in range(start_line - 1):
                line = f.readline()
                if not line:  # EOF
                    break
                current_line += 1

            # Read lines for this range
            while end_line is None or current_line <= end_line:
                line = f.readline()
                if not line:  # EOF
                    break

                try:
                    record = orjson.loads(line.strip())
                    records.append(record)
                except orjson.JSONDecodeError as e:
                    if not skip_errors:
                        raise SerializationError(
                            f"Invalid JSON on line {current_line}: {e}",
                            original_error=e,
                        )
                    # Skip silently in multiprocessing to avoid logging issues
                except Exception as e:
                    if not skip_errors:
                        raise ProcessingError(
                            f"Error parsing line {current_line}: {e}",
                            original_error=e,
                        )
                    # Skip silently in multiprocessing to avoid logging issues

                current_line += 1

        # Process the entire chunk at once
        if records and chunk_handler is not None:
            try:
                if include_batch_info and batch_info is not None:
                    result = chunk_handler(records, batch_info)
                else:
                    result = chunk_handler(records)
                if return_results:
                    return result
            except Exception as e:
                if not skip_errors:
                    error_msg = f"Chunk handler failed for chunk {start}-{end}"
                    if batch_info:
                        error_msg = f"Chunk handler failed for batch {batch_info.batch_id}"
                    raise ProcessingError(error_msg + f": {e}", original_error=e)
                # Skip silently in multiprocessing to avoid logging issues

    except (OSError, IOError) as e:
        raise FileHandlingError(f"Failed to process chunk in file {filepath}: {e}") from e

    return None
