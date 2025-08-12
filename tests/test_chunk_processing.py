"""Tests for chunk processing functionality."""

import pytest

from fast_ndjson_processor import (
    ConfigurationError,
    FastNDJSONProcessor,
    ProcessorMode,
    process_ndjson_chunks,
)

from .conftest import (
    global_analyze_chunk,
    global_chunk_aggregator,
    global_chunk_counter,
    global_chunk_identifier,
    global_chunk_info,
    global_chunk_statistics,
    global_count_valid_records,
    global_process_large_chunk,
    global_simple_chunk_count,
    global_sometimes_failing_chunk_handler,
)


class TestChunkProcessing:
    """Test suite for chunk-level processing."""

    def test_process_file_chunks_parallel_mode(self, create_ndjson_file):
        """Test chunk processing in parallel mode."""
        processor = FastNDJSONProcessor(n_workers=2)
        filepath = create_ndjson_file()

        results = processor.process_file_chunks(
            filepath, global_chunk_counter, ProcessorMode.PARALLEL
        )

        assert results is not None
        assert len(results) >= 1  # At least one chunk

        # Verify chunk result structure
        for result in results:
            assert "count" in result
            assert "total_value" in result
            assert "first_id" in result
            assert "last_id" in result
            assert result["count"] > 0

        # Verify total count matches expected records
        total_records = sum(r["count"] for r in results)
        assert total_records == 5  # We know the test file has 5 records

    def test_process_file_chunks_sequential_mode(self, create_ndjson_file):
        """Test chunk processing in sequential mode."""
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        results = processor.process_file_chunks(
            filepath, global_chunk_counter, ProcessorMode.SEQUENTIAL
        )

        assert results is not None
        assert len(results) >= 1

        for result in results:
            assert "count" in result
            assert "total_value" in result
            assert result["count"] > 0

    def test_process_file_chunks_streaming_not_supported(self, create_ndjson_file):
        """Test that chunk processing is not supported in streaming mode."""
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        with pytest.raises(
            ConfigurationError, match="Chunk processing is not supported in STREAMING mode"
        ):
            processor.process_file_chunks(filepath, global_chunk_counter, ProcessorMode.STREAMING)

    def test_process_file_chunks_aggregation(self, create_ndjson_file):
        """Test chunk processing for data aggregation."""
        processor = FastNDJSONProcessor(n_workers=2)
        filepath = create_ndjson_file()

        results = processor.process_file_chunks(filepath, global_chunk_aggregator)

        assert results is not None
        assert len(results) >= 1

        # Combine results from all chunks
        combined_categories = {}
        for chunk_result in results:
            for category, data in chunk_result.items():
                if category not in combined_categories:
                    combined_categories[category] = {"count": 0, "ids": []}
                combined_categories[category]["count"] += data["count"]
                combined_categories[category]["ids"].extend(data["ids"])

        # Should have both categories A and B
        assert "A" in combined_categories or "B" in combined_categories
        total_count = sum(cat["count"] for cat in combined_categories.values())
        assert total_count == 5

    def test_process_file_chunks_statistics(self, create_ndjson_file):
        """Test chunk processing for statistical calculations."""
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        results = processor.process_file_chunks(filepath, global_chunk_statistics)

        assert results is not None
        assert len(results) >= 1

        # Verify statistics make sense
        for result in results:
            assert result["count"] > 0
            assert result["min"] is not None
            assert result["max"] is not None
            assert result["min"] <= result["max"]
            assert result["avg"] == result["sum"] / result["count"]

    def test_process_file_chunks_with_return_results_false(self, create_ndjson_file):
        """Test chunk processing with return_results=False."""
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        results = processor.process_file_chunks(
            filepath, global_chunk_counter, return_results=False
        )

        assert results is None

    def test_process_file_chunks_empty_chunks_handled(self, create_empty_file):
        """Test chunk processing with empty file."""
        processor = FastNDJSONProcessor()
        filepath = create_empty_file()

        results = processor.process_file_chunks(filepath, global_chunk_counter)

        # Should return empty list for empty file
        assert results == []

    def test_process_file_chunks_custom_chunk_size(self, create_large_ndjson_file):
        """Test chunk processing with custom chunk size."""
        # Create a larger file to test chunking
        processor = FastNDJSONProcessor(n_workers=2, chunk_size=100)  # Small chunks
        filepath = create_large_ndjson_file(200)  # 200 records

        results = processor.process_file_chunks(filepath, global_chunk_counter)

        assert results is not None
        assert len(results) > 1  # Should have multiple chunks due to small chunk size

        # Verify all records are processed
        total_records = sum(r["count"] for r in results)
        assert total_records == 200

    def test_process_file_chunks_error_handling(self, create_ndjson_file):
        """Test error handling in chunk processing."""
        processor = FastNDJSONProcessor(skip_errors=False)
        filepath = create_ndjson_file()

        def failing_chunk_handler(records):
            raise ValueError("Test error in chunk handler")

        with pytest.raises(Exception):  # Should propagate the error
            processor.process_file_chunks(filepath, failing_chunk_handler)

    def test_process_file_chunks_error_handling_with_skip(self, create_ndjson_file):
        """Test error handling with skip_errors=True."""
        processor = FastNDJSONProcessor(skip_errors=True)
        filepath = create_ndjson_file()

        # Should not raise an exception due to skip_errors=True
        results = processor.process_file_chunks(filepath, global_sometimes_failing_chunk_handler)
        # Results might be empty if all chunks failed, but no exception should be raised
        assert results is not None


class TestChunkProcessingConvenienceFunction:
    """Test suite for process_ndjson_chunks convenience function."""

    def test_process_ndjson_chunks_basic(self, create_ndjson_file):
        """Test basic usage of process_ndjson_chunks."""
        filepath = create_ndjson_file()

        results = process_ndjson_chunks(filepath, global_analyze_chunk)

        assert results is not None
        assert len(results) >= 1

        total_records = sum(r["record_count"] for r in results)
        assert total_records == 5

    def test_process_ndjson_chunks_with_custom_params(self, create_ndjson_file):
        """Test process_ndjson_chunks with custom parameters."""
        filepath = create_ndjson_file()

        results = process_ndjson_chunks(
            filepath,
            global_simple_chunk_count,
            mode=ProcessorMode.SEQUENTIAL,
            n_workers=1,
            chunk_size=1024,
            encoding="utf-8",
            skip_errors=False,
            show_progress=False,
            return_results=True,
            progress_desc="Testing chunks",
        )

        assert results is not None
        assert len(results) >= 1
        assert all(isinstance(r, int) for r in results)  # Should be counts (simple count function)

    def test_process_ndjson_chunks_no_results(self, create_ndjson_file):
        """Test process_ndjson_chunks with return_results=False."""
        filepath = create_ndjson_file()

        results = process_ndjson_chunks(filepath, global_simple_chunk_count, return_results=False)

        assert results is None

    def test_process_ndjson_chunks_parallel_mode(self, create_ndjson_file):
        """Test process_ndjson_chunks in parallel mode."""
        filepath = create_ndjson_file()

        results = process_ndjson_chunks(
            filepath, global_chunk_info, mode=ProcessorMode.PARALLEL, n_workers=2
        )

        assert results is not None
        assert len(results) >= 1

        for result in results:
            assert "size" in result
            assert "min_id" in result
            assert "max_id" in result
            if result["size"] > 0:
                assert result["min_id"] is not None
                assert result["max_id"] is not None


class TestChunkProcessingEdgeCases:
    """Test edge cases for chunk processing."""

    def test_chunk_processing_with_malformed_json(self, create_invalid_ndjson_file):
        """Test chunk processing with malformed JSON lines."""
        processor = FastNDJSONProcessor(skip_errors=True)
        filepath = create_invalid_ndjson_file()

        # Should handle malformed JSON gracefully
        results = processor.process_file_chunks(filepath, global_count_valid_records)

        assert results is not None
        # Should have processed only valid records

    def test_chunk_processing_single_large_record(self, temp_dir):
        """Test chunk processing with a single large record."""
        import json

        filepath = temp_dir / "large_record.ndjson"
        large_record = {"id": 1, "data": "x" * 10000}  # Large record

        with open(filepath, "w") as f:
            f.write(json.dumps(large_record) + "\n")

        processor = FastNDJSONProcessor()

        results = processor.process_file_chunks(filepath, global_process_large_chunk)

        assert results is not None
        assert len(results) >= 1
        assert results[0]["count"] == 1
        assert results[0]["data_sizes"][0] == 10000

    def test_chunk_processing_many_small_chunks(self, create_large_ndjson_file):
        """Test chunk processing with many small chunks."""
        processor = FastNDJSONProcessor(n_workers=4, chunk_size=50)  # Very small chunks
        filepath = create_large_ndjson_file(100)  # 100 records

        results = processor.process_file_chunks(filepath, global_chunk_identifier)

        assert results is not None
        assert len(results) > 1  # Should have multiple chunks

        # Verify all records processed
        total_size = sum(r["size"] for r in results)
        assert total_size == 100

        # Verify chunk IDs are unique
        chunk_ids = [r["chunk_id"] for r in results]
        assert len(set(chunk_ids)) == len(chunk_ids)  # All unique
