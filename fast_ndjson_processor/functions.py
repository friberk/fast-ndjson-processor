from pathlib import Path
from typing import Callable, Iterator, List, Optional, Union

from .fast_ndjson_processor import (
    ChunkHandlerReturn,
    ChunkType,
    FastNDJSONProcessor,
    HandlerReturn,
    RecordType,
)
from .processor_mode import ProcessorMode


# Convenience functions
def process_ndjson(
    filepath: Union[str, Path],
    handler: Callable[[RecordType], HandlerReturn],
    mode: ProcessorMode = ProcessorMode.PARALLEL,
    n_workers: Optional[int] = None,
    chunk_size: Optional[int] = None,
    encoding: str = "utf-8",
    skip_errors: bool = True,
    show_progress: bool = False,
    return_results: bool = True,
    progress_desc: str = "Processing chunks",
) -> Optional[List[HandlerReturn]]:
    """
    Quick function to process an NDJSON file.

    Args:
        filepath: Path to NDJSON file
        handler: Processing function for each record
        mode: Processing mode (default: ProcessorMode.PARALLEL)
        n_workers: Number of worker processes (defaults to CPU count)
        chunk_size: Size of chunks in bytes (auto-calculated if None)
        encoding: File encoding (default: 'utf-8')
        skip_errors: Whether to skip malformed JSON lines (default: True)
        show_progress: Show progress bar if tqdm is available (default: False)
        return_results: Whether to return results (default: True)
        progress_desc: Description for progress bar (default: 'Processing chunks')

    Returns:
        List of processed results if return_results=True, None otherwise
    """
    processor = FastNDJSONProcessor(
        n_workers=n_workers,
        chunk_size=chunk_size,
        encoding=encoding,
        skip_errors=skip_errors,
        show_progress=show_progress,
    )
    return processor.process_file(
        filepath=filepath,
        handler=handler,
        mode=mode,
        return_results=return_results,
        progress_desc=progress_desc,
    )


def stream_ndjson(
    filepath: Union[str, Path],
    handler: Optional[Callable[[RecordType], HandlerReturn]] = None,
    encoding: str = "utf-8",
    skip_errors: bool = True,
) -> Iterator[Union[RecordType, HandlerReturn]]:
    """
    Stream an NDJSON file record by record.

    Args:
        filepath: Path to NDJSON file
        handler: Optional function to process each record
        encoding: File encoding (default: 'utf-8')
        skip_errors: Whether to skip malformed JSON lines (default: True)

    Yields:
        Processed records if handler provided, otherwise raw JSON objects
    """
    processor = FastNDJSONProcessor(encoding=encoding, skip_errors=skip_errors)
    yield from processor.stream_file(filepath, handler)


def process_ndjson_chunks(
    filepath: Union[str, Path],
    chunk_handler: Callable[[ChunkType], ChunkHandlerReturn],
    mode: ProcessorMode = ProcessorMode.PARALLEL,
    n_workers: Optional[int] = None,
    chunk_size: Optional[int] = None,
    encoding: str = "utf-8",
    skip_errors: bool = True,
    show_progress: bool = False,
    return_results: bool = True,
    progress_desc: str = "Processing chunks",
) -> Optional[List[ChunkHandlerReturn]]:
    """
    Quick function to process an NDJSON file with chunk-level processing.

    Each worker receives a list of records (chunk) to process at once,
    allowing for custom batch operations, aggregations, or transformations.

    Args:
        filepath: Path to NDJSON file
        chunk_handler: Processing function for each chunk (list of records)
        mode: Processing mode (default: ProcessorMode.PARALLEL)
        n_workers: Number of worker processes (defaults to CPU count)
        chunk_size: Size of chunks in bytes (auto-calculated if None)
        encoding: File encoding (default: 'utf-8')
        skip_errors: Whether to skip malformed JSON lines (default: True)
        show_progress: Show progress bar if tqdm is available (default: False)
        return_results: Whether to return results (default: True)
        progress_desc: Description for progress bar (default: 'Processing chunks')

    Returns:
        List of chunk processing results if return_results=True, None otherwise

    Example:
        >>> def analyze_chunk(records):
        ...     return {
        ...         "count": len(records),
        ...         "avg_value": sum(r.get("value", 0) for r in records) / len(records),
        ...         "unique_ids": list(set(r["id"] for r in records))
        ...     }
        >>> results = process_ndjson_chunks("data.ndjson", analyze_chunk)
    """
    processor = FastNDJSONProcessor(
        n_workers=n_workers,
        chunk_size=chunk_size,
        encoding=encoding,
        skip_errors=skip_errors,
        show_progress=show_progress,
    )
    return processor.process_file_chunks(
        filepath=filepath,
        chunk_handler=chunk_handler,
        mode=mode,
        return_results=return_results,
        progress_desc=progress_desc,
    )
