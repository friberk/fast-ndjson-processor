from pathlib import Path
from typing import Any, BinaryIO, Callable, Dict, List, Optional, Union

import orjson
from filelock import FileLock

from .exceptions import ConfigurationError, ValidationError, WriterError
from .writer_mode import WriterMode

# Type aliases for better readability
JSONData = Union[Dict[str, Any], List[Dict[str, Any]]]
SerializationDefault = Optional[Callable[[Any], Any]]


class FastNDJSONWriter:
    """
    High-performance writer for creating NDJSON files.

    Examples:
        Basic usage:
        >>> with FastNDJSONWriter("output.ndjson") as writer:
        ...     writer.write({"id": 1, "name": "Alice"})
        ...     writer.write({"id": 2, "name": "Bob"})

        Batch writing:
        >>> with FastNDJSONWriter("output.ndjson") as writer:
        ...     writer.save([{"id": 1}, {"id": 2}])

        Append mode:
        >>> with FastNDJSONWriter("output.ndjson", WriterMode.APPEND) as writer:
        ...     writer.append({"id": 3, "name": "Charlie"})

        Concurrent access with file locking:
        >>> with FastNDJSONWriter("output.ndjson", use_file_lock=True) as writer:
        ...     writer.write({"id": 1, "name": "Alice"})
    """

    def __init__(
        self,
        filepath: Union[str, Path],
        mode: WriterMode = WriterMode.WRITE,
        use_file_lock: bool = False,
    ) -> None:
        """
        Initialize NDJSON writer.

        Args:
            filepath: Output file path
            mode: Mode for writer (default: WriterMode.WRITE)
            use_file_lock: Enable file locking for concurrent access (default: False)

        Raises:
            ConfigurationError: If mode is not a valid WriterMode
        """
        self.filepath = Path(filepath)

        # Raise error if mode is not valid
        if not isinstance(mode, WriterMode):
            raise ConfigurationError(f"Invalid mode: {mode}. Must be one of {list(WriterMode)}")

        self.mode = mode
        self.use_file_lock = use_file_lock
        self._file: Optional[BinaryIO] = None
        self._file_lock: Optional["FileLock"] = None

    def __enter__(self) -> "FastNDJSONWriter":
        """Context manager entry."""
        try:
            # Ensure parent directory exists
            self.filepath.parent.mkdir(parents=True, exist_ok=True)

            # Setup file lock if requested
            if self.use_file_lock:
                # Use .lock extension for lock file
                lock_path = str(self.filepath) + ".lock"
                self._file_lock = FileLock(lock_path)
                self._file_lock.acquire()

            # Open the actual file
            self._file = open(self.filepath, self.mode.value + "b")

            return self
        except Exception as e:
            # Clean up on error
            if self._file_lock:
                try:
                    self._file_lock.release()
                except Exception:
                    pass
            if self._file:
                self._file.close()
            raise WriterError(f"Failed to open file {self.filepath}: {e}") from e

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        # Close file first
        if self._file:
            self._file.close()

        # Release lock after file is closed
        if self._file_lock:
            try:
                self._file_lock.release()
            except Exception:
                # Ignore errors during cleanup
                pass

    def _ensure_open(self) -> None:
        """Ensure the file is open for writing."""
        if not self._file or self._file.closed:
            raise WriterError("Writer not opened. Use 'with FastNDJSONWriter(...) as writer:'")

    def write(
        self,
        data: Dict[str, Any],
        default: SerializationDefault = None,
        option: int = 0,
    ) -> None:
        """
        Write a single JSON record to the file.

        Args:
            data: A dictionary to be serialized and written to the file
            default: Optional callable for custom serialization
            option: Optional orjson option flags (default: 0)

        Raises:
            ValidationError: If data is not a dictionary
            WriterError: If writer is not opened
        """
        self._ensure_open()

        if not isinstance(data, dict):
            raise ValidationError(f"Expected dict, got {type(data).__name__}")

        self._file.write(orjson.dumps(data, default=default, option=option))
        self._file.write(b"\n")

    def save(
        self,
        data: JSONData,
        default: SerializationDefault = None,
        option: int = 0,
    ) -> None:
        """
        Serialize Python objects to a NDJSON file.

        Save will overwrite the file if it already exists or create a new file if it doesn't exist.

        Args:
            data: A dictionary or list of dictionaries to be serialized to the file
            default: Optional callable passed to orjson.dumps() as the 'default' argument
                    that serializes subclasses or arbitrary types to supported types
            option: Optional integer passed to orjson.dumps() as the 'option' argument
                   that modifies how data is serialized (default: 0)

        Raises:
            WriterError: If used with append mode
            ValidationError: If data contains non-dict items in a list
            WriterError: If writer is not opened
        """
        if self.mode == WriterMode.APPEND:
            raise WriterError("Saving is not supported for append mode. Use append() instead.")

        # Ensure writer is open
        self._ensure_open()

        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    raise ValidationError(
                        f"Invalid data type in list: {type(item).__name__}. Expected dict."
                    )
                self._file.write(orjson.dumps(item, default=default, option=option))
                self._file.write(b"\n")
        elif isinstance(data, dict):
            self._file.write(orjson.dumps(data, default=default, option=option))
            self._file.write(b"\n")
        else:
            raise ValidationError(
                f"Invalid data type: {type(data).__name__}. Expected dict or list of dicts."
            )

    def append(
        self,
        data: JSONData,
        newline: bool = True,
        default: SerializationDefault = None,
        option: int = 0,
    ) -> None:
        """
        Serialize and append Python objects to a NDJSON file.

        Append will append to the file if it already exists or create a new file if it doesn't exist.

        Args:
            data: A dictionary or list of dictionaries to be serialized and appended to the file
            newline: Boolean flag that, if set to False, indicates that the file does not end
                    with a newline and should have one added before data is appended (default: True)
            default: Optional callable passed to orjson.dumps() as the 'default' argument
                    that serializes subclasses or arbitrary types to supported types
            option: Optional integer passed to orjson.dumps() as the 'option' argument
                   that modifies how data is serialized (default: 0)

        Raises:
            WriterError: If used with write mode
            ValidationError: If data contains non-dict items in a list
            WriterError: If writer is not opened
        """
        if self.mode == WriterMode.WRITE:
            raise WriterError("Appending is not supported for write mode. Use save() instead.")

        # Ensure writer is open
        self._ensure_open()

        if not newline:
            self._file.write(b"\n")

        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    raise ValidationError(
                        f"Invalid data type in list: {type(item).__name__}. Expected dict."
                    )
                self._file.write(orjson.dumps(item, default=default, option=option))
                self._file.write(b"\n")
        elif isinstance(data, dict):
            self._file.write(orjson.dumps(data, default=default, option=option))
            self._file.write(b"\n")
        else:
            raise ValidationError(
                f"Invalid data type: {type(data).__name__}. Expected dict or list of dicts."
            )

    def write_batch(
        self,
        data: List[Dict[str, Any]],
        default: SerializationDefault = None,
        option: int = 0,
    ) -> None:
        """
        Write multiple records efficiently in a batch.

        Args:
            data: List of dictionaries to write
            default: Optional callable for custom serialization
            option: Optional orjson option flags (default: 0)

        Raises:
            ValidationError: If data is not a list or contains non-dict items
            WriterError: If writer is not opened
        """
        self._ensure_open()

        if not isinstance(data, list):
            raise ValidationError(f"Expected list, got {type(data).__name__}")

        for item in data:
            if not isinstance(item, dict):
                raise ValidationError(
                    f"Invalid data type in batch: {type(item).__name__}. Expected dict."
                )
            self._file.write(orjson.dumps(item, default=default, option=option))
            self._file.write(b"\n")
