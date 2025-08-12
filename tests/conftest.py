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


@pytest.fixture
def create_large_ndjson_file(temp_dir):
    """Create a large NDJSON file for testing."""

    def _create_file(num_records: int = 1000, filename: str = "large.ndjson") -> Path:
        filepath = temp_dir / filename
        with open(filepath, "w") as f:
            for i in range(num_records):
                record = {
                    "id": i,
                    "name": f"User{i}",
                    "value": i * 2.5,
                    "category": f"cat_{i % 5}",
                    "active": i % 2 == 0,
                }
                f.write(json.dumps(record) + "\n")
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


# Global chunk handlers for testing (must be pickleable)
def global_chunk_counter(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Count records in a chunk."""
    return {
        "count": len(records),
        "total_value": sum(r.get("id", 0) for r in records),
        "first_id": records[0]["id"] if records else None,
        "last_id": records[-1]["id"] if records else None,
    }


def global_chunk_aggregator(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate records by category."""
    categories = {}
    for record in records:
        # Create a mock category based on id
        category = "A" if record["id"] % 2 == 0 else "B"
        if category not in categories:
            categories[category] = {"count": 0, "ids": []}
        categories[category]["count"] += 1
        categories[category]["ids"].append(record["id"])

    return categories


def global_chunk_statistics(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate statistics for a chunk."""
    if not records:
        return {"count": 0, "min": None, "max": None, "sum": 0}

    ids = [r["id"] for r in records]
    return {
        "count": len(ids),
        "min": min(ids),
        "max": max(ids),
        "sum": sum(ids),
        "avg": sum(ids) / len(ids),
    }


def global_simple_chunk_count(records: List[Dict[str, Any]]) -> int:
    """Return simple count of records."""
    return len(records)


def global_chunk_info(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Return basic chunk information."""
    return {
        "size": len(records),
        "min_id": min(r["id"] for r in records) if records else None,
        "max_id": max(r["id"] for r in records) if records else None,
    }


def global_analyze_chunk(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze a chunk of records."""
    return {
        "record_count": len(records),
        "id_sum": sum(r["id"] for r in records),
    }


def global_sometimes_failing_chunk_handler(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Sometimes fail based on chunk characteristics."""
    if len(records) > 10:  # Fail on large chunks
        raise ValueError("Test error")
    return {"count": len(records)}


def global_count_valid_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Count valid records."""
    return {"valid_count": len(records)}


def global_process_large_chunk(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Process chunk with large records."""
    return {
        "count": len(records),
        "data_sizes": [len(r.get("data", "")) for r in records],
    }


def global_chunk_identifier(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Identify chunk by record IDs."""
    return {
        "chunk_id": f"{records[0]['id']}-{records[-1]['id']}" if records else "empty",
        "size": len(records),
    }


@pytest.fixture
def failing_handler():
    """Handler that always fails for error testing."""
    return global_failing_handler


@pytest.fixture
def flexible_handler():
    """Handler that works with various record structures."""
    return global_flexible_handler


# Chunk handler fixtures
@pytest.fixture
def chunk_counter():
    """Chunk handler that counts records."""
    return global_chunk_counter


@pytest.fixture
def chunk_aggregator():
    """Chunk handler that aggregates records."""
    return global_chunk_aggregator


@pytest.fixture
def chunk_statistics():
    """Chunk handler that calculates statistics."""
    return global_chunk_statistics
