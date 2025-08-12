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


@runtime_checkable
class JSONRecord(Protocol):
    """Protocol for JSON record types."""

    pass


RecordType = Dict[str, Any]
HandlerReturn = Any

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
    ) -> None:
        """
        Initialize the NDJSON processor.

        Args:
            n_workers: Number of worker processes (defaults to CPU count)
            chunk_size: Size of chunks in bytes (auto-calculated if None)
            encoding: File encoding (default: 'utf-8')
            skip_errors: Whether to skip malformed JSON lines (default: True)
            show_progress: Show progress bar if tqdm is available (default: False)

        Raises:
            ValueError: If n_workers or chunk_size are negative
        """
        if n_workers is not None and n_workers <= 0:
            raise ConfigurationError("n_workers must be positive")
        if chunk_size is not None and chunk_size <= 0:
            raise ConfigurationError("chunk_size must be positive")

        self.n_workers = n_workers or mp.cpu_count()
        self.chunk_size = chunk_size
        self.encoding = encoding
        self.skip_errors = skip_errors
        self.show_progress = show_progress

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
            results = self._process_parallel(filepath, handler, return_results, progress_desc)
        elif mode == ProcessorMode.SEQUENTIAL:
            results = self._process_sequential(filepath, handler, return_results, progress_desc)
        elif mode == ProcessorMode.STREAMING:
            results = list(self.stream_file(filepath, handler)) if return_results else None
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
        handler: Callable[[RecordType], HandlerReturn],
        return_results: bool = True,
        progress_desc: str = "Processing records",
    ) -> Optional[List[HandlerReturn]]:
        """Process file sequentially in a single thread."""
        results = [] if return_results else None

        if not filepath.exists():
            raise FileHandlingError(f"File not found: {filepath}")
        if not filepath.is_file():
            raise FileHandlingError(f"Path is not a file: {filepath}")

        try:
            with open(filepath, "r", encoding=self.encoding) as f:
                line_number = 0

                # Count total lines for progress bar if needed
                if self.show_progress:
                    total_lines = sum(1 for _ in f)
                    f.seek(0)
                    line_iter = tqdm(f, total=total_lines, desc=progress_desc)
                else:
                    line_iter = f

                for line in line_iter:
                    line_number += 1
                    try:
                        parsed = orjson.loads(line.strip())
                        result = handler(parsed)
                        if return_results and result is not None:
                            results.append(result)
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

        return results

    def _process_parallel(
        self,
        filepath: Path,
        handler: Callable[[RecordType], HandlerReturn],
        return_results: bool = True,
        progress_desc: str = "Processing chunks",
    ) -> Optional[List[HandlerReturn]]:
        """Process file in parallel using multiple processes."""
        chunks = self._calculate_chunks(filepath)

        # Prepare to collect results
        all_results = [] if return_results else None

        # Validate chunks before starting multiprocessing
        if not chunks:
            logger.warning(f"No chunks to process for file {filepath}")
            return all_results

        with mp.Pool(processes=self.n_workers) as pool:
            worker_func = partial(
                _process_chunk,
                filepath=str(filepath),
                handler=handler,
                encoding=self.encoding,
                skip_errors=self.skip_errors,
                return_results=return_results,
            )

            # Create iterator that will yield results
            result_iter = pool.imap_unordered(worker_func, chunks)

            # Process results as they arrive to minimize memory usage
            if self.show_progress:
                result_iter = tqdm(result_iter, total=len(chunks), desc=progress_desc)

            # Process results as they come in
            for result in result_iter:
                if return_results and result is not None:
                    all_results.extend(result)

        return all_results

    def _calculate_chunks(self, filepath: Path) -> List[Tuple[int, int]]:
        """Calculate chunk boundaries aligned with newlines."""
        file_size = filepath.stat().st_size

        # Handle empty files
        if file_size == 0:
            return []

        if self.chunk_size:
            chunk_size = self.chunk_size
        else:
            chunk_size = max(file_size // self.n_workers, 1024 * 1024)  # Min 1MB chunks

        chunks = []
        with open(filepath, "rb") as f:
            positions = []
            pos = 0

            while pos < file_size:
                positions.append(pos)
                pos += chunk_size

            # Align positions to newline boundaries
            aligned_positions = [0]
            for pos in positions[1:]:
                aligned_pos = self._find_newline_boundary(f, min(pos, file_size - 1))
                if aligned_pos > aligned_positions[-1]:
                    aligned_positions.append(aligned_pos)

            if aligned_positions[-1] < file_size:
                aligned_positions.append(file_size)

            # Create chunk tuples
            for i in range(len(aligned_positions) - 1):
                chunks.append((aligned_positions[i], aligned_positions[i + 1]))

        return chunks

    def _find_newline_boundary(self, f: io.BufferedReader, pos: int) -> int:
        """Find the nearest newline boundary from position."""
        # Get file size once
        file_size = f.seek(0, 2)
        f.seek(pos)

        # Read forward to find next newline
        while pos < file_size:
            char = f.read(1)
            if not char:  # EOF reached
                break
            if char == b"\n":
                return pos + 1
            pos += 1

        return pos


def _process_chunk(
    chunk: Tuple[int, int],
    filepath: str,
    handler: Callable[[RecordType], HandlerReturn],
    encoding: str,
    skip_errors: bool,
    return_results: bool = True,
) -> Optional[List[HandlerReturn]]:
    """Worker function to process a chunk of the file."""
    start, end = chunk
    results = [] if return_results else None

    try:
        with open(filepath, "r", encoding=encoding) as f:
            f.seek(start)
            current_pos = start
            line_number = 0

            for line in f:
                # Calculate the actual byte position before processing
                line_bytes = line.encode(encoding)
                if current_pos + len(line_bytes) > end:
                    break

                current_pos += len(line_bytes)
                line_number += 1

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
                            f"Invalid JSON in chunk at position {current_pos}: {e}",
                            original_error=e,
                        )
                    # Skip silently in multiprocessing to avoid logging issues
                except Exception as e:
                    if not skip_errors:
                        raise ProcessingError(
                            f"Handler failed in chunk at position {current_pos}: {e}",
                            original_error=e,
                        )
                    # Skip silently in multiprocessing to avoid logging issues
    except (OSError, IOError) as e:
        raise FileHandlingError(f"Failed to process chunk in file {filepath}: {e}") from e

    if return_results:
        return results
    else:
        return None
