"""Shared test fixtures and configuration."""

import json
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def sample_data() -> List[Dict[str, Any]]:
    """Sample JSON records for testing."""
    return [
        {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
        {"id": 2, "name": "Bob", "age": 25, "city": "San Francisco"},
        {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"},
        {"id": 4, "name": "Diana", "age": 28, "city": "Boston"},
        {"id": 5, "name": "Eve", "age": 32, "city": "Seattle"},
    ]


@pytest.fixture
def large_sample_data() -> List[Dict[str, Any]]:
    """Large dataset for performance testing."""
    return [
        {"id": i, "name": f"User{i}", "value": i * 2, "category": f"cat_{i % 10}"}
        for i in range(10000)
    ]


@pytest.fixture
def create_ndjson_file(temp_dir, sample_data):
    """Create a test NDJSON file."""

    def _create_file(data: List[Dict[str, Any]] = None, filename: str = "test.ndjson") -> Path:
        if data is None:
            data = sample_data

        filepath = temp_dir / filename
        with open(filepath, "w") as f:
            for record in data:
                f.write(json.dumps(record) + "\n")
        return filepath

    return _create_file


@pytest.fixture
def create_invalid_ndjson_file(temp_dir):
    """Create an NDJSON file with invalid JSON lines."""

    def _create_file(filename: str = "invalid.ndjson") -> Path:
        filepath = temp_dir / filename
        with open(filepath, "w") as f:
            f.write('{"valid": "json"}\n')
            f.write("invalid json line\n")
            f.write('{"another": "valid"}\n')
            f.write('{"incomplete": \n')
            f.write('{"final": "record"}\n')
        return filepath

    return _create_file


@pytest.fixture
def create_empty_file(temp_dir):
    """Create an empty file."""

    def _create_file(filename: str = "empty.ndjson") -> Path:
        filepath = temp_dir / filename
        filepath.touch()
        return filepath

    return _create_file


def global_mock_handler(record: Dict[str, Any]) -> Dict[str, Any]:
    """Global mock handler function for testing (pickleable)."""
    return {"processed_id": record["id"], "processed": True}


@pytest.fixture
def mock_handler():
    """Mock handler function for testing."""
    return global_mock_handler


def global_failing_handler(record: Dict[str, Any]) -> Dict[str, Any]:
    """Global failing handler function for testing (pickleable)."""
    raise ValueError("Handler intentionally failed")


def global_flexible_handler(record: Dict[str, Any]) -> Dict[str, Any]:
    """Global flexible handler that works with any record structure (pickleable)."""
    record_id = record.get("id", record.get("another", "unknown"))
    return {"processed_id": record_id, "processed": True}


@pytest.fixture
def failing_handler():
    """Handler that always fails for error testing."""
    return global_failing_handler


@pytest.fixture
def flexible_handler():
    """Handler that works with various record structures."""
    return global_flexible_handler
