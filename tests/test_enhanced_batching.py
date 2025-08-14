#!/usr/bin/env python3
"""
Proper tests for the enhanced batching capabilities of FastNDJSONProcessor.

Tests verify that:
1. number_of_batches creates exactly the specified number of batches
2. start_line/end_line processes only the specified line range
3. BatchInfo contains correct metadata
4. Batch distribution is correct
5. All records are processed exactly once
"""

import json
import tempfile
import unittest
from pathlib import Path
from fast_ndjson_processor import FastNDJSONProcessor
from fast_ndjson_processor.processor_mode import ProcessorMode


# Global handler functions for parallel processing tests (needed for pickling)
def global_batch_id_handler(records, batch_info):
    """Handler that returns batch_id for testing."""
    global_batch_id_handler.batch_ids.append(batch_info.batch_id)
    return len(records)

def global_batch_info_handler(records, batch_info):
    """Handler that returns batch_info for testing."""
    global_batch_info_handler.batch_infos.append(batch_info)
    return batch_info

def global_record_collector_handler(records, batch_info):
    """Handler that collects all records for testing."""
    global_record_collector_handler.all_records.extend(records)
    return len(records)

def global_batch_size_handler(records, batch_info):
    """Handler that returns batch size for testing."""
    global_batch_size_handler.batch_sizes.append(batch_info.batch_size)
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


class TestEnhancedBatching(unittest.TestCase):

    def setUp(self):
        """Create test data for each test."""
        self.test_file = None
        self.test_data = []

    def tearDown(self):
        """Clean up test files."""
        if self.test_file and self.test_file.exists():
            self.test_file.unlink()

    def create_test_file(self, num_lines=100):
        """Create a test NDJSON file with specified number of lines."""
        self.test_file = Path(tempfile.mktemp(suffix='.ndjson'))
        self.test_data = []

        with open(self.test_file, 'w') as f:
            for i in range(1, num_lines + 1):
                data = {
                    "id": i,
                    "line_number": i,
                    "value": i * 10,
                    "category": "A" if i % 2 == 0 else "B"
                }
                self.test_data.append(data)
                f.write(json.dumps(data) + '\n')

        return self.test_file

    def test_number_of_batches_exact_count(self):
        """Test that number_of_batches creates exactly the specified number of batches."""
        test_file = self.create_test_file(100)
        processor = FastNDJSONProcessor(number_of_batches=8)

        def batch_handler(records, batch_info):
            return {
                "batch_id": batch_info.batch_id,
                "batch_size": batch_info.batch_size,
                "actual_records": len(records),
                "record_ids": [r["id"] for r in records]
            }

        results = processor.process_file_chunks(
            test_file, batch_handler, mode=ProcessorMode.SEQUENTIAL, include_batch_info=True
        )

        # Should have exactly 8 batches
        self.assertEqual(len(results), 8)

        # Batch IDs should be 0-7
        batch_ids = [r["batch_id"] for r in results]
        self.assertEqual(sorted(batch_ids), list(range(8)))

        # All records should be processed exactly once
        all_record_ids = []
        for result in results:
            all_record_ids.extend(result["record_ids"])

        self.assertEqual(len(all_record_ids), 100)
        self.assertEqual(sorted(all_record_ids), list(range(1, 101)))

    def test_line_range_processing(self):
        """Test that start_line/end_line processes only the specified range."""
        test_file = self.create_test_file(100)
        processor = FastNDJSONProcessor(start_line=21, end_line=30, number_of_batches=3)

        def batch_handler(records, batch_info):
            return {
                "batch_id": batch_info.batch_id,
                "batch_start_line": batch_info.batch_start_line,
                "batch_end_line": batch_info.batch_end_line,
                "record_ids": [r["id"] for r in records]
            }

        results = processor.process_file_chunks(
            test_file, batch_handler, mode=ProcessorMode.SEQUENTIAL, include_batch_info=True
        )

        # Should have 3 batches
        self.assertEqual(len(results), 3)

        # All processed records should be in range 21-30
        all_record_ids = []
        for result in results:
            all_record_ids.extend(result["record_ids"])

        self.assertEqual(len(all_record_ids), 10)  # Lines 21-30 = 10 lines
        self.assertEqual(sorted(all_record_ids), list(range(21, 31)))

        # Check that batch line ranges are correct
        all_lines_covered = set()
        for result in results:
            start = result["batch_start_line"]
            end = result["batch_end_line"]
            self.assertGreaterEqual(start, 21)
            self.assertLessEqual(end, 30)
            for line in range(start, end + 1):
                all_lines_covered.add(line)

        # All lines 21-30 should be covered exactly once
        self.assertEqual(all_lines_covered, set(range(21, 31)))

    def test_batch_info_metadata(self):
        """Test that BatchInfo contains correct metadata."""
        test_file = self.create_test_file(50)
        processor = FastNDJSONProcessor(number_of_batches=5)

        def batch_handler(records, batch_info):
            # Verify batch_size matches actual records
            expected_size = batch_info.batch_end_line - batch_info.batch_start_line + 1
            self.assertEqual(batch_info.batch_size, expected_size)
            self.assertEqual(len(records), expected_size)

            # Verify record IDs match line numbers
            for i, record in enumerate(records):
                expected_line = batch_info.batch_start_line + i
                self.assertEqual(record["line_number"], expected_line)

            return {
                "batch_id": batch_info.batch_id,
                "verified": True
            }

        results = processor.process_file_chunks(
            test_file, batch_handler, mode=ProcessorMode.SEQUENTIAL, include_batch_info=True
        )

        # All batches should be verified
        self.assertEqual(len(results), 5)
        for result in results:
            self.assertTrue(result["verified"])

    def test_batch_distribution(self):
        """Test that batches are distributed as evenly as possible."""
        test_file = self.create_test_file(100)
        processor = FastNDJSONProcessor(number_of_batches=7)  # 100/7 = ~14.3

        def batch_handler(records, batch_info):
            return {
                "batch_id": batch_info.batch_id,
                "size": len(records)
            }

        results = processor.process_file_chunks(
            test_file, batch_handler, mode=ProcessorMode.SEQUENTIAL, include_batch_info=True
        )

        sizes = [r["size"] for r in results]

        # With 100 lines and 7 batches: 100 // 7 = 14, 100 % 7 = 2
        # So we should have 2 batches of size 15 and 5 batches of size 14
        self.assertEqual(sorted(sizes), [14, 14, 14, 14, 14, 15, 15])
        self.assertEqual(sum(sizes), 100)

    def test_backward_compatibility(self):
        """Test that existing functionality still works without batch info."""
        test_file = self.create_test_file(50)
        processor = FastNDJSONProcessor(n_workers=4)  # Use n_workers instead of number_of_batches

        # Old-style chunk handler (no batch info)
        def chunk_handler(records):
            return {
                "count": len(records),
                "sum_values": sum(r["value"] for r in records)
            }

        # Should work without include_batch_info=True
        results = processor.process_file_chunks(test_file, chunk_handler, mode=ProcessorMode.SEQUENTIAL)

        # With small file, might create fewer chunks than workers
        self.assertGreaterEqual(len(results), 1)
        total_count = sum(r["count"] for r in results)
        self.assertEqual(total_count, 50)

    def test_single_batch(self):
        """Test edge case with single batch."""
        test_file = self.create_test_file(10)
        processor = FastNDJSONProcessor(number_of_batches=1)

        def batch_handler(records, batch_info):
            return {
                "batch_id": batch_info.batch_id,
                "batch_start_line": batch_info.batch_start_line,
                "batch_end_line": batch_info.batch_end_line,
                "count": len(records)
            }

        results = processor.process_file_chunks(
            test_file, batch_handler, mode=ProcessorMode.SEQUENTIAL, include_batch_info=True
        )

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result["batch_id"], 0)
        self.assertEqual(result["batch_start_line"], 1)
        self.assertEqual(result["batch_end_line"], 10)
        self.assertEqual(result["count"], 10)

    def test_more_batches_than_lines(self):
        """Test when number_of_batches exceeds number of lines."""
        test_file = self.create_test_file(3)
        processor = FastNDJSONProcessor(number_of_batches=10)

        def batch_handler(records, batch_info):
            return {"batch_id": batch_info.batch_id, "count": len(records)}

        results = processor.process_file_chunks(
            test_file, batch_handler, mode=ProcessorMode.SEQUENTIAL, include_batch_info=True
        )

        # Should only create 3 batches (one per line)
        self.assertEqual(len(results), 3)

    def test_invalid_line_ranges(self):
        """Test error handling for invalid line ranges."""
        test_file = self.create_test_file(10)

        # start_line > end_line
        with self.assertRaises(Exception):
            FastNDJSONProcessor(start_line=5, end_line=3)

        # start_line exceeds file length
        processor = FastNDJSONProcessor(start_line=15, end_line=20)
        with self.assertRaises(Exception):
            processor.process_file_chunks(test_file, lambda r, b: r, include_batch_info=True)

if __name__ == "__main__":
    unittest.main()
