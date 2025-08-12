"""
Fast NDJSON Processor

A high-performance Python library for processing NDJSON (Newline Delimited JSON) files
with multiprocessing support, streaming capabilities, and comprehensive error handling.

Example usage:
    Basic processing:
    >>> from fast_ndjson_processor import FastNDJSONProcessor
    >>> processor = FastNDJSONProcessor()
    >>> results = processor.process_file("data.ndjson", lambda x: x["id"])

    Convenience functions:
    >>> from fast_ndjson_processor import process_ndjson, stream_ndjson
    >>> results = process_ndjson("data.ndjson", lambda x: x["id"])
    >>> for record in stream_ndjson("data.ndjson"):
    ...     print(record)

    Writing NDJSON:
    >>> from fast_ndjson_processor import FastNDJSONWriter
    >>> with FastNDJSONWriter("output.ndjson") as writer:
    ...     writer.write({"id": 1, "name": "Alice"})
"""

from .exceptions import (
    ConfigurationError,
    FastNDJSONError,
    FileHandlingError,
    ProcessingError,
    SerializationError,
    ValidationError,
    WriterError,
)
from .fast_ndjson_processor import FastNDJSONProcessor
from .fast_ndjson_writer import FastNDJSONWriter
from .functions import process_ndjson, process_ndjson_chunks, stream_ndjson
from .processor_mode import ProcessorMode
from .writer_mode import WriterMode

__version__ = "1.0.0"
__author__ = "Berk Çakar"

__all__ = [
    # Main classes
    "FastNDJSONProcessor",
    "FastNDJSONWriter",
    # Convenience functions
    "process_ndjson",
    "stream_ndjson",
    "process_ndjson_chunks",
    # Enums
    "ProcessorMode",
    "WriterMode",
    # Exceptions
    "FastNDJSONError",
    "ProcessingError",
    "ValidationError",
    "FileHandlingError",
    "SerializationError",
    "WriterError",
    "ConfigurationError",
    # Metadata
    "__version__",
    "__author__",
    "__email__",
]
