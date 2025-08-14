"""Tests for FastNDJSONProcessor."""

import pytest

from fast_ndjson_processor import (
    ConfigurationError,
    FastNDJSONProcessor,
    FileHandlingError,
    ProcessingError,
    ProcessorMode,
    SerializationError,
)


class TestFastNDJSONProcessor:
    """Test suite for FastNDJSONProcessor."""

    def test_init_default_values(self):
        """Test processor initialization with default values."""
        processor = FastNDJSONProcessor()
        assert processor.n_workers > 0
        assert processor.chunk_size is None
        assert processor.encoding == "utf-8"
        assert processor.skip_errors is True
        assert processor.show_progress is False

    def test_init_custom_values(self):
        """Test processor initialization with custom values."""
        processor = FastNDJSONProcessor(
            n_workers=4, chunk_size=2048, encoding="latin-1", skip_errors=False, show_progress=True
        )
        assert processor.n_workers == 4
        assert processor.chunk_size == 2048
        assert processor.encoding == "latin-1"
        assert processor.skip_errors is False

    def test_init_invalid_n_workers(self):
        """Test processor initialization with invalid n_workers."""
        with pytest.raises(ConfigurationError, match="n_workers must be positive"):
            FastNDJSONProcessor(n_workers=0)

        with pytest.raises(ConfigurationError, match="n_workers must be positive"):
            FastNDJSONProcessor(n_workers=-1)

    def test_init_invalid_chunk_size(self):
        """Test processor initialization with invalid chunk_size."""
        with pytest.raises(ConfigurationError, match="chunk_size must be positive"):
            FastNDJSONProcessor(chunk_size=0)

        with pytest.raises(ConfigurationError, match="chunk_size must be positive"):
            FastNDJSONProcessor(chunk_size=-1)

    def test_process_file_parallel_mode(self, create_ndjson_file, mock_handler):
        """Test file processing in parallel mode."""
        processor = FastNDJSONProcessor(n_workers=2)
        filepath = create_ndjson_file()

        results = processor.process_file(filepath, mock_handler, ProcessorMode.PARALLEL)

        assert len(results) == 5
        assert all("processed_id" in result for result in results)

    def test_process_file_sequential_mode(self, create_ndjson_file, mock_handler):
        """Test file processing in sequential mode."""
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        results = processor.process_file(filepath, mock_handler, ProcessorMode.SEQUENTIAL)

        assert len(results) == 5
        assert all("processed_id" in result for result in results)

    def test_process_file_streaming_mode(self, create_ndjson_file, mock_handler):
        """Test file processing in streaming mode."""
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        results = processor.process_file(filepath, mock_handler, ProcessorMode.STREAMING)

        assert len(results) == 5
        assert all("processed_id" in result for result in results)

    def test_process_file_nonexistent_file(self, temp_dir, mock_handler):
        """Test processing nonexistent file."""
        processor = FastNDJSONProcessor()
        nonexistent_file = temp_dir / "nonexistent.ndjson"

        with pytest.raises(FileHandlingError, match="File not found"):
            processor.process_file(nonexistent_file, mock_handler)

    def test_process_file_invalid_mode(self, create_ndjson_file, mock_handler):
        """Test processing with invalid mode."""
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        with pytest.raises(ConfigurationError, match="Invalid processing mode"):
            processor.process_file(filepath, mock_handler, "invalid_mode")

    def test_stream_file_basic(self, create_ndjson_file):
        """Test basic file streaming."""
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        records = list(processor.stream_file(filepath))

        assert len(records) == 5
        assert all("id" in record for record in records)

    def test_stream_file_with_handler(self, create_ndjson_file, mock_handler):
        """Test file streaming with handler."""
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        results = list(processor.stream_file(filepath, mock_handler))

        assert len(results) == 5
        assert all("processed_id" in result for result in results)

    def test_stream_file_invalid_json_skip_errors(self, create_invalid_ndjson_file):
        """Test streaming with invalid JSON and skip_errors=True."""
        processor = FastNDJSONProcessor(skip_errors=True)
        filepath = create_invalid_ndjson_file()

        records = list(processor.stream_file(filepath))

        # Should get 3 valid records (skipping 2 invalid ones)
        assert len(records) == 3

    def test_stream_file_invalid_json_no_skip(self, create_invalid_ndjson_file):
        """Test streaming with invalid JSON and skip_errors=False."""
        processor = FastNDJSONProcessor(skip_errors=False)
        filepath = create_invalid_ndjson_file()

        with pytest.raises(SerializationError):
            list(processor.stream_file(filepath))

    def test_stream_file_handler_failure_skip_errors(self, create_ndjson_file, failing_handler):
        """Test streaming with failing handler and skip_errors=True."""
        processor = FastNDJSONProcessor(skip_errors=True)
        filepath = create_ndjson_file()

        # Should not raise, but will skip all records due to handler failures
        records = list(processor.stream_file(filepath, failing_handler))
        assert len(records) == 0

    def test_stream_file_handler_failure_no_skip(self, create_ndjson_file, failing_handler):
        """Test streaming with failing handler and skip_errors=False."""
        processor = FastNDJSONProcessor(skip_errors=False)
        filepath = create_ndjson_file()

        with pytest.raises(ProcessingError):
            list(processor.stream_file(filepath, failing_handler))

    def test_empty_file_processing(self, create_empty_file, mock_handler):
        """Test processing empty file."""
        processor = FastNDJSONProcessor()
        filepath = create_empty_file()

        results = processor.process_file(filepath, mock_handler, ProcessorMode.PARALLEL)
        assert results == []

    def test_calculate_chunks_empty_file(self, create_empty_file):
        """Test chunk calculation for empty file."""
        processor = FastNDJSONProcessor()
        filepath = create_empty_file()

        chunks = processor._calculate_chunks(filepath)
        assert chunks == []

    def test_calculate_chunks_small_file(self, create_ndjson_file):
        """Test chunk calculation for small file."""
        processor = FastNDJSONProcessor(n_workers=4)
        filepath = create_ndjson_file()

        chunks = processor._calculate_chunks(filepath)
        assert len(chunks) >= 1
        assert all(chunk_start_line <= chunk_end_line for _, chunk_start_line, chunk_end_line, _, _ in chunks)

    def test_progress_bar_integration(self, create_ndjson_file, mock_handler):
        """Test progress bar integration."""
        processor = FastNDJSONProcessor(show_progress=True)
        filepath = create_ndjson_file()

        # Just make sure it runs without error when show_progress=True
        results = processor.process_file(filepath, mock_handler, ProcessorMode.SEQUENTIAL)
        assert len(results) == 5

    def test_large_file_processing(self, create_ndjson_file, large_sample_data, mock_handler):
        """Test processing large file."""
        processor = FastNDJSONProcessor(n_workers=2, chunk_size=1024)
        filepath = create_ndjson_file(large_sample_data, "large.ndjson")

        results = processor.process_file(filepath, mock_handler, ProcessorMode.PARALLEL)

        assert len(results) == 10000
        assert all("processed_id" in result for result in results)

    def test_return_results_false(self, create_ndjson_file, mock_handler):
        """Test processing with return_results=False."""
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        results = processor.process_file(
            filepath, mock_handler, ProcessorMode.PARALLEL, return_results=False
        )

        assert results is None

    def test_streaming_mode_processing(self, create_ndjson_file, mock_handler):
        """Test processing in streaming mode."""
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        results = processor.process_file(filepath, mock_handler, ProcessorMode.STREAMING)

        assert len(results) == 5
        assert all("processed_id" in result for result in results)

    def test_sequential_return_results_false(self, create_ndjson_file, mock_handler):
        """Test sequential processing with return_results=False."""
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        results = processor.process_file(
            filepath, mock_handler, ProcessorMode.SEQUENTIAL, return_results=False
        )

        assert results is None
