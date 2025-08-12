from enum import Enum


class ProcessorMode(Enum):
    """Processing modes for NDJSON files."""

    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"
    STREAMING = "streaming"
