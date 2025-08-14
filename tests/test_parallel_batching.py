#!/usr/bin/env python3
"""
Tests for parallel processing with enhanced batching capabilities.
"""

import json
import tempfile
import unittest
import os
import shutil
from fast_ndjson_processor import FastNDJSONProcessor
from fast_ndjson_processor.processor_mode import ProcessorMode


# Global handler functions for parallel processing tests (needed for pickling)
def global_batch_id_handler(records, batch_info):
    """Handler that returns batch_id for testing."""
    return batch_info.batch_id

def global_batch_info_handler(records, batch_info):
    """Handler that returns batch_info for testing."""
    return batch_info

def global_record_count_handler(records, batch_info):
    """Handler that returns record count for testing."""
    return len(records)

def global_batch_size_handler(records, batch_info):
    """Handler that returns batch size for testing."""
    return batch_info.batch_size

def global_chunk_handler_simple(records):
    """Simple handler for backward compatibility testing."""
    return len(records)

def global_failing_handler(records, batch_info):
    """Handler that fails on batch_id 1 for error testing."""
    if batch_info.batch_id == 1:
        raise ValueError("Test error")
    return len(records)

def global_single_batch_validator(records, batch_info):
    """Handler that validates single batch properties."""
    assert batch_info.batch_id == 0
    assert batch_info.batch_start_line == 1
    assert batch_info.batch_end_line == 100
    assert batch_info.batch_size == 100
    return len(records)

def global_record_return_handler(records, batch_info):
    """Handler that returns the actual records for testing."""
    return records


class TestParallelBatching(unittest.TestCase):
    """Test enhanced batching features with parallel processing."""

    def setUp(self):
        """Set up test files."""
        self.temp_dir = tempfile.mkdtemp()

        # Create test file with 100 records
        self.test_file = os.path.join(self.temp_dir, "test.ndjson")
        with open(self.test_file, "w") as f:
            for i in range(100):
                json.dump({"id": i, "value": f"item_{i}"}, f)
                f.write("\n")

    def tearDown(self):
        """Clean up test files."""
        shutil.rmtree(self.temp_dir)

    def test_parallel_number_of_batches_exact_count(self):
        """Test that parallel processing creates the exact number of batches requested."""
        processor = FastNDJSONProcessor(
            number_of_batches=5,
            n_workers=3
        )

        # Use handler that returns batch_id
        batch_ids = processor.process_file_chunks(
            self.test_file,
            global_batch_id_handler,
            mode=ProcessorMode.PARALLEL,
            include_batch_info=True
        )

        # Use handler that returns record count
        record_counts = processor.process_file_chunks(
            self.test_file,
            global_record_count_handler,
            mode=ProcessorMode.PARALLEL,
            include_batch_info=True
        )

        # Should have exactly 5 batches
        self.assertEqual(len(batch_ids), 5)
        self.assertEqual(len(set(batch_ids)), 5)  # Unique batch IDs
        self.assertEqual(sum(record_counts), 100)  # Total records processed

    def test_parallel_batch_info_metadata(self):
        """Test that batch metadata is correct in parallel processing."""
        processor = FastNDJSONProcessor(
            number_of_batches=4,
            n_workers=2
        )

        results = processor.process_file_chunks(
            self.test_file,
            global_batch_info_handler,
            mode=ProcessorMode.PARALLEL,
            include_batch_info=True
        )

        # Sort results by batch_id for consistent testing
        results.sort(key=lambda x: x.batch_id)

        # Check batch IDs are sequential
        for i, batch_info in enumerate(results):
            self.assertEqual(batch_info.batch_id, i)
            self.assertGreater(batch_info.batch_size, 0)
            self.assertGreaterEqual(batch_info.batch_start_line, 1)
            self.assertLessEqual(batch_info.batch_end_line, 100)
            self.assertLessEqual(batch_info.batch_start_line, batch_info.batch_end_line)

    def test_parallel_line_range_processing(self):
        """Test parallel processing with specific line ranges."""
        processor = FastNDJSONProcessor(
            start_line=20,
            end_line=60,
            number_of_batches=3,
            n_workers=2
        )

        # Get record counts per batch
        record_counts = processor.process_file_chunks(
            self.test_file,
            global_record_count_handler,
            mode=ProcessorMode.PARALLEL,
            include_batch_info=True
        )

        # Get actual records from all batches
        record_batches = processor.process_file_chunks(
            self.test_file,
            global_record_return_handler,
            mode=ProcessorMode.PARALLEL,
            include_batch_info=True
        )

        # Should process exactly 41 lines (20-60 inclusive)
        self.assertEqual(sum(record_counts), 41)

        # Flatten all records from all batches
        all_records = []
        for batch in record_batches:
            all_records.extend(batch)

        self.assertEqual(len(all_records), 41)

        # Check that we got the right records (IDs 19-59, since file is 0-indexed)
        ids = [record["id"] for record in all_records]
        ids.sort()
        self.assertEqual(ids, list(range(19, 60)))

    def test_parallel_batch_distribution(self):
        """Test that parallel processing distributes batches evenly."""
        processor = FastNDJSONProcessor(
            number_of_batches=7,
            n_workers=3
        )

        batch_sizes = processor.process_file_chunks(
            self.test_file,
            global_batch_size_handler,
            mode=ProcessorMode.PARALLEL,
            include_batch_info=True
        )

        # All batch sizes should be close (difference of at most 1)
        min_size = min(batch_sizes)
        max_size = max(batch_sizes)
        self.assertLessEqual(max_size - min_size, 1)

        # Total should equal 100
        self.assertEqual(sum(batch_sizes), 100)

    def test_parallel_more_workers_than_batches(self):
        """Test parallel processing when n_workers > number_of_batches."""
        processor = FastNDJSONProcessor(
            number_of_batches=2,
            n_workers=8
        )

        batch_ids = processor.process_file_chunks(
            self.test_file,
            global_batch_id_handler,
            mode=ProcessorMode.PARALLEL,
            include_batch_info=True
        )

        record_counts = processor.process_file_chunks(
            self.test_file,
            global_record_count_handler,
            mode=ProcessorMode.PARALLEL,
            include_batch_info=True
        )

        # Should still have exactly 2 batches (not 8)
        self.assertEqual(len(batch_ids), 2)
        self.assertEqual(len(set(batch_ids)), 2)
        self.assertEqual(sum(record_counts), 100)

    def test_parallel_single_batch(self):
        """Test parallel processing with a single batch."""
        processor = FastNDJSONProcessor(
            number_of_batches=1,
            n_workers=4
        )

        results = processor.process_file_chunks(
            self.test_file,
            global_single_batch_validator,
            mode=ProcessorMode.PARALLEL,
            include_batch_info=True
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], 100)

    def test_parallel_backward_compatibility(self):
        """Test that parallel processing works without include_batch_info."""
        processor = FastNDJSONProcessor(
            number_of_batches=3,
            n_workers=2
        )

        results = processor.process_file_chunks(
            self.test_file,
            global_chunk_handler_simple,
            mode=ProcessorMode.PARALLEL,
            include_batch_info=False  # Default value
        )

        # Should still work and process all records
        self.assertEqual(len(results), 3)
        self.assertEqual(sum(results), 100)

    def test_parallel_error_handling(self):
        """Test parallel processing with error handling."""
        processor = FastNDJSONProcessor(
            number_of_batches=3,
            n_workers=2,
            skip_errors=True
        )

        # Should handle errors gracefully with skip_errors=True
        results = processor.process_file_chunks(
            self.test_file,
            global_failing_handler,
            mode=ProcessorMode.PARALLEL,
            include_batch_info=True
        )

        # Should get results from successful batches only
        self.assertLess(len(results), 3)  # Some batches failed
        self.assertGreater(sum(results), 0)  # But some succeeded


if __name__ == "__main__":
    unittest.main()
