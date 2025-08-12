"""Tests for edge cases and error conditions."""

from unittest.mock import patch

import pytest

from fast_ndjson_processor import (
    FastNDJSONProcessor,
    FastNDJSONWriter,
    FileHandlingError,
    ProcessorMode,
    SerializationError,
    WriterError,
    WriterMode,
)


class TestEdgeCasesProcessor:
    """Test edge cases for FastNDJSONProcessor."""

    def test_stream_file_path_is_directory(self, temp_dir):
        """Test streaming when path is a directory."""
        processor = FastNDJSONProcessor()

        with pytest.raises(FileHandlingError, match="Path is not a file"):
            list(processor.stream_file(temp_dir))

    def test_process_file_path_is_directory(self, temp_dir, mock_handler):
        """Test processing when path is a directory."""
        processor = FastNDJSONProcessor()

        with pytest.raises(FileHandlingError, match="Path is not a file"):
            processor.process_file(temp_dir, mock_handler)

    @patch("fast_ndjson_processor.fast_ndjson_processor.open")
    def test_stream_file_io_error(self, mock_file_open, create_ndjson_file):
        """Test stream_file with IO error."""
        mock_file_open.side_effect = IOError("Mock IO error")
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        with pytest.raises(FileHandlingError, match="Failed to read file"):
            list(processor.stream_file(filepath))

    @patch("fast_ndjson_processor.fast_ndjson_processor.open")
    def test_sequential_processing_io_error(self, mock_file_open, create_ndjson_file, mock_handler):
        """Test sequential processing with IO error."""
        mock_file_open.side_effect = OSError("Mock OS error")
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()

        with pytest.raises(FileHandlingError, match="Failed to read file"):
            processor.process_file(filepath, mock_handler, ProcessorMode.SEQUENTIAL)

    def test_sequential_json_decode_error_no_skip(
        self, create_invalid_ndjson_file, flexible_handler
    ):
        """Test sequential processing with JSON decode error and skip_errors=False."""
        processor = FastNDJSONProcessor(skip_errors=False)
        filepath = create_invalid_ndjson_file()

        with pytest.raises(SerializationError, match="Invalid JSON on line"):
            processor.process_file(filepath, flexible_handler, ProcessorMode.SEQUENTIAL)

    def test_sequential_handler_error_no_skip(self, create_ndjson_file, failing_handler):
        """Test sequential processing with handler error and skip_errors=False."""
        processor = FastNDJSONProcessor(skip_errors=False)
        filepath = create_ndjson_file()

        with pytest.raises(Exception):  # ProcessingError or the original ValueError
            processor.process_file(filepath, failing_handler, ProcessorMode.SEQUENTIAL)

    def test_sequential_json_decode_error_with_skip(
        self, create_invalid_ndjson_file, flexible_handler
    ):
        """Test sequential processing with JSON decode error and skip_errors=True."""
        processor = FastNDJSONProcessor(skip_errors=True)
        filepath = create_invalid_ndjson_file()

        results = processor.process_file(filepath, flexible_handler, ProcessorMode.SEQUENTIAL)

        # Should get 3 valid records (skipping 2 invalid ones)
        assert len(results) == 3

    def test_sequential_handler_error_with_skip(self, create_ndjson_file, failing_handler):
        """Test sequential processing with handler error and skip_errors=True."""
        processor = FastNDJSONProcessor(skip_errors=True)
        filepath = create_ndjson_file()

        results = processor.process_file(filepath, failing_handler, ProcessorMode.SEQUENTIAL)

        # Should get empty list as all handlers fail
        assert results == []

    def test_calculate_chunks_with_custom_chunk_size(self, create_ndjson_file):
        """Test chunk calculation with custom chunk size."""
        processor = FastNDJSONProcessor(n_workers=2, chunk_size=50)
        filepath = create_ndjson_file()

        chunks = processor._calculate_chunks(filepath)

        # Should have multiple chunks due to small chunk size
        assert len(chunks) >= 2
        assert all(start < end for start, end in chunks)

    def test_find_newline_boundary_at_file_end(self, temp_dir):
        """Test _find_newline_boundary when seeking beyond file end."""
        # Create a small file without trailing newline
        filepath = temp_dir / "small.ndjson"
        with open(filepath, "w") as f:
            f.write('{"id": 1}')

        processor = FastNDJSONProcessor()

        with open(filepath, "rb") as f:
            # Test boundary finding beyond file end
            file_size = f.seek(0, 2)  # Get file size
            boundary = processor._find_newline_boundary(f, file_size + 10)
            # When pos > file_size, it should return the position passed in
            assert boundary == file_size + 10


class TestEdgeCasesWriter:
    """Test edge cases for FastNDJSONWriter."""

    @patch("fast_ndjson_processor.fast_ndjson_writer.open")
    def test_context_manager_open_error(self, mock_file_open, temp_dir):
        """Test context manager with file open error."""
        mock_file_open.side_effect = OSError("Mock OS error")
        filepath = temp_dir / "test.ndjson"

        with pytest.raises(WriterError, match="Failed to open file"):
            with FastNDJSONWriter(filepath):
                pass

    def test_write_without_context_manager_after_close(self, temp_dir):
        """Test writing after context manager has closed."""
        filepath = temp_dir / "test.ndjson"

        with FastNDJSONWriter(filepath) as writer:
            writer.write({"id": 1})

        # Now try to write after context manager closed
        with pytest.raises(WriterError, match="Writer not opened"):
            writer.write({"id": 2})

    def test_append_to_file_with_trailing_newline(self, temp_dir):
        """Test appending to file that already has trailing newline."""
        filepath = temp_dir / "test.ndjson"

        # Create file with trailing newline
        with open(filepath, "w") as f:
            f.write('{"id": 1}\n')  # Has newline at end

        with FastNDJSONWriter(filepath, WriterMode.APPEND) as writer:
            writer.append({"id": 2})  # Should automatically add newline

        with open(filepath, "r") as f:
            lines = f.readlines()
            assert len(lines) == 2
            assert '"id": 1' in lines[0] or '"id":1' in lines[0]
            assert '"id": 2' in lines[1] or '"id":2' in lines[1]


class TestMultiprocessingEdgeCases:
    """Test edge cases in multiprocessing scenarios."""

    def test_chunk_processing_with_encoding_issues(self, temp_dir, mock_handler):
        """Test chunk processing with encoding edge cases."""
        # Create file with different line lengths to test chunk boundaries
        filepath = temp_dir / "varied_lines.ndjson"
        with open(filepath, "w", encoding="utf-8") as f:
            f.write('{"id": 1, "short": "a"}\n')
            f.write('{"id": 2, "medium": "this is a medium length string"}\n')
            f.write('{"id": 3, "long": "' + "x" * 100 + '"}\n')
            f.write('{"id": 4, "short": "b"}\n')

        processor = FastNDJSONProcessor(n_workers=2, chunk_size=100)
        results = processor.process_file(filepath, mock_handler, ProcessorMode.PARALLEL)

        assert len(results) == 4
        assert all("processed_id" in result for result in results)
