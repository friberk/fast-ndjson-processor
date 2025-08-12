"""Tests for convenience functions."""

from fast_ndjson_processor import ProcessorMode, process_ndjson, stream_ndjson


class TestConvenienceFunctions:
    """Test suite for convenience functions."""

    def test_process_ndjson_basic(self, create_ndjson_file, mock_handler):
        """Test basic process_ndjson function."""
        filepath = create_ndjson_file()

        results = process_ndjson(filepath, mock_handler)

        assert len(results) == 5
        assert all("processed_id" in result for result in results)

    def test_process_ndjson_with_custom_params(self, create_ndjson_file, mock_handler):
        """Test process_ndjson with custom parameters."""
        filepath = create_ndjson_file()

        results = process_ndjson(
            filepath,
            mock_handler,
            mode=ProcessorMode.SEQUENTIAL,
            n_workers=1,
            chunk_size=1024,
            encoding="utf-8",
            skip_errors=False,
            show_progress=False,
            return_results=True,
            progress_desc="Custom processing",
        )

        assert len(results) == 5
        assert all("processed_id" in result for result in results)

    def test_process_ndjson_no_results(self, create_ndjson_file, mock_handler):
        """Test process_ndjson with return_results=False."""
        filepath = create_ndjson_file()

        results = process_ndjson(filepath, mock_handler, return_results=False)

        assert results is None

    def test_stream_ndjson_basic(self, create_ndjson_file):
        """Test basic stream_ndjson function."""
        filepath = create_ndjson_file()

        records = list(stream_ndjson(filepath))

        assert len(records) == 5
        assert all("id" in record for record in records)

    def test_stream_ndjson_with_handler(self, create_ndjson_file, mock_handler):
        """Test stream_ndjson with handler."""
        filepath = create_ndjson_file()

        results = list(stream_ndjson(filepath, handler=mock_handler))

        assert len(results) == 5
        assert all("processed_id" in result for result in results)

    def test_stream_ndjson_with_custom_params(self, create_ndjson_file):
        """Test stream_ndjson with custom parameters."""
        filepath = create_ndjson_file()

        records = list(stream_ndjson(filepath, handler=None, encoding="utf-8", skip_errors=True))

        assert len(records) == 5
        assert all("id" in record for record in records)

    def test_stream_ndjson_skip_errors(self, create_invalid_ndjson_file):
        """Test stream_ndjson with skip_errors=True."""
        filepath = create_invalid_ndjson_file()

        records = list(stream_ndjson(filepath, skip_errors=True))

        # Should get 3 valid records (skipping 2 invalid ones)
        assert len(records) == 3
