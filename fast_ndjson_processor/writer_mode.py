from enum import Enum


class WriterMode(Enum):
    """
    Mode for FastNDJSONWriter.
    """

    WRITE = "w"
    APPEND = "a"
