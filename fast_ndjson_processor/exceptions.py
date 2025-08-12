"""Custom exceptions for the fast_ndjson_processor library."""

from typing import Any, Optional


class FastNDJSONError(Exception):
    """Base exception for all fast_ndjson_processor errors."""

    pass


class ProcessingError(FastNDJSONError):
    """Raised when an error occurs during NDJSON processing."""

    def __init__(
        self,
        message: str,
        line_number: Optional[int] = None,
        original_error: Optional[Exception] = None,
    ):
        """
        Initialize ProcessingError.

        Args:
            message: Error message
            line_number: Line number where the error occurred (if known)
            original_error: The original exception that caused this error
        """
        self.line_number = line_number
        self.original_error = original_error

        if line_number is not None:
            message = f"Line {line_number}: {message}"

        super().__init__(message)


class ValidationError(FastNDJSONError):
    """Raised when input validation fails."""

    pass


class FileHandlingError(FastNDJSONError):
    """Raised when file operations fail."""

    pass


class SerializationError(FastNDJSONError):
    """Raised when JSON serialization/deserialization fails."""

    def __init__(
        self, message: str, data: Optional[Any] = None, original_error: Optional[Exception] = None
    ):
        """
        Initialize SerializationError.

        Args:
            message: Error message
            data: The data that failed to serialize (if applicable)
            original_error: The original exception that caused this error
        """
        self.data = data
        self.original_error = original_error
        super().__init__(message)


class WriterError(FastNDJSONError):
    """Raised when NDJSON writer operations fail."""

    pass


class ConfigurationError(FastNDJSONError):
    """Raised when invalid configuration is provided."""

    pass
