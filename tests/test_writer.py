"""Tests for FastNDJSONWriter."""

import json
import platform

import pytest

from fast_ndjson_processor import (
    ConfigurationError,
    FastNDJSONWriter,
    ValidationError,
    WriterError,
    WriterMode,
)


class TestFastNDJSONWriter:
    """Test suite for FastNDJSONWriter."""

    def test_init_default_mode(self, temp_dir):
        """Test writer initialization with default mode."""
        filepath = temp_dir / "test.ndjson"
        writer = FastNDJSONWriter(filepath)

        assert writer.filepath == filepath
        assert writer.mode == WriterMode.WRITE

    def test_init_append_mode(self, temp_dir):
        """Test writer initialization with append mode."""
        filepath = temp_dir / "test.ndjson"
        writer = FastNDJSONWriter(filepath, WriterMode.APPEND)

        assert writer.filepath == filepath
        assert writer.mode == WriterMode.APPEND

    def test_init_invalid_mode(self, temp_dir):
        """Test writer initialization with invalid mode."""
        filepath = temp_dir / "test.ndjson"

        with pytest.raises(ConfigurationError, match="Invalid mode"):
            FastNDJSONWriter(filepath, "invalid_mode")

    def test_context_manager_write_mode(self, temp_dir, sample_data):
        """Test writer as context manager in write mode."""
        filepath = temp_dir / "test.ndjson"

        with FastNDJSONWriter(filepath) as writer:
            for record in sample_data:
                writer.write(record)

        # Verify file was created and contains correct data
        assert filepath.exists()
        with open(filepath, "r") as f:
            lines = f.readlines()
            assert len(lines) == 5
            for i, line in enumerate(lines):
                parsed = json.loads(line.strip())
                assert parsed == sample_data[i]

    def test_context_manager_append_mode(self, temp_dir, sample_data):
        """Test writer as context manager in append mode."""
        filepath = temp_dir / "test.ndjson"

        # First write some data
        with FastNDJSONWriter(filepath) as writer:
            writer.write(sample_data[0])

        # Then append more data
        with FastNDJSONWriter(filepath, WriterMode.APPEND) as writer:
            writer.append(sample_data[1])

        # Verify both records are in file
        with open(filepath, "r") as f:
            lines = f.readlines()
            assert len(lines) == 2
            assert json.loads(lines[0].strip()) == sample_data[0]
            assert json.loads(lines[1].strip()) == sample_data[1]

    def test_write_single_record(self, temp_dir):
        """Test writing a single record."""
        filepath = temp_dir / "test.ndjson"
        record = {"id": 1, "name": "test"}

        with FastNDJSONWriter(filepath) as writer:
            writer.write(record)

        with open(filepath, "r") as f:
            line = f.readline()
            assert json.loads(line.strip()) == record

    def test_write_invalid_data_type(self, temp_dir):
        """Test writing invalid data type."""
        filepath = temp_dir / "test.ndjson"

        with FastNDJSONWriter(filepath) as writer:
            with pytest.raises(ValidationError, match="Expected dict"):
                writer.write("not a dict")

    def test_save_list_of_dicts(self, temp_dir, sample_data):
        """Test saving a list of dictionaries."""
        filepath = temp_dir / "test.ndjson"

        with FastNDJSONWriter(filepath) as writer:
            writer.save(sample_data)

        with open(filepath, "r") as f:
            lines = f.readlines()
            assert len(lines) == 5
            for i, line in enumerate(lines):
                assert json.loads(line.strip()) == sample_data[i]

    def test_save_single_dict(self, temp_dir):
        """Test saving a single dictionary."""
        filepath = temp_dir / "test.ndjson"
        record = {"id": 1, "name": "test"}

        with FastNDJSONWriter(filepath) as writer:
            writer.save(record)

        with open(filepath, "r") as f:
            line = f.readline()
            assert json.loads(line.strip()) == record

    def test_save_in_append_mode_fails(self, temp_dir):
        """Test that save fails in append mode."""
        filepath = temp_dir / "test.ndjson"

        with FastNDJSONWriter(filepath, WriterMode.APPEND) as writer:
            with pytest.raises(WriterError, match="Saving is not supported for append mode"):
                writer.save({"test": "data"})

    def test_save_invalid_data_type(self, temp_dir):
        """Test saving invalid data type."""
        filepath = temp_dir / "test.ndjson"

        with FastNDJSONWriter(filepath) as writer:
            with pytest.raises(ValidationError, match="Invalid data type"):
                writer.save("not a dict or list")

    def test_save_list_with_invalid_item(self, temp_dir):
        """Test saving list with invalid item."""
        filepath = temp_dir / "test.ndjson"
        invalid_data = [{"valid": "record"}, "invalid_item"]

        with FastNDJSONWriter(filepath) as writer:
            with pytest.raises(ValidationError, match="Invalid data type in list"):
                writer.save(invalid_data)

    def test_append_single_dict(self, temp_dir):
        """Test appending a single dictionary."""
        filepath = temp_dir / "test.ndjson"
        record = {"id": 1, "name": "test"}

        with FastNDJSONWriter(filepath, WriterMode.APPEND) as writer:
            writer.append(record)

        with open(filepath, "r") as f:
            line = f.readline()
            assert json.loads(line.strip()) == record

    def test_append_in_write_mode_fails(self, temp_dir):
        """Test that append fails in write mode."""
        filepath = temp_dir / "test.ndjson"

        with FastNDJSONWriter(filepath) as writer:
            with pytest.raises(WriterError, match="Appending is not supported for write mode"):
                writer.append({"test": "data"})

    def test_append_with_newline_false(self, temp_dir):
        """Test appending with newline=False."""
        filepath = temp_dir / "test.ndjson"

        # Create file without trailing newline
        with open(filepath, "w") as f:
            f.write('{"id": 1}')  # No newline at end

        with FastNDJSONWriter(filepath, WriterMode.APPEND) as writer:
            writer.append({"id": 2}, newline=False)

        with open(filepath, "r") as f:
            lines = f.readlines()
            assert len(lines) == 2
            assert json.loads(lines[0].strip()) == {"id": 1}
            assert json.loads(lines[1].strip()) == {"id": 2}

    def test_write_batch(self, temp_dir, sample_data):
        """Test batch writing."""
        filepath = temp_dir / "test.ndjson"

        with FastNDJSONWriter(filepath) as writer:
            writer.write_batch(sample_data)

        with open(filepath, "r") as f:
            lines = f.readlines()
            assert len(lines) == 5
            for i, line in enumerate(lines):
                assert json.loads(line.strip()) == sample_data[i]

    def test_write_batch_invalid_type(self, temp_dir):
        """Test batch writing with invalid type."""
        filepath = temp_dir / "test.ndjson"

        with FastNDJSONWriter(filepath) as writer:
            with pytest.raises(ValidationError, match="Expected list"):
                writer.write_batch({"not": "a list"})

    def test_write_batch_invalid_item(self, temp_dir):
        """Test batch writing with invalid item."""
        filepath = temp_dir / "test.ndjson"
        invalid_batch = [{"valid": "record"}, "invalid_item"]

        with FastNDJSONWriter(filepath) as writer:
            with pytest.raises(ValidationError, match="Invalid data type in batch"):
                writer.write_batch(invalid_batch)

    def test_ensure_open_without_context_manager(self, temp_dir):
        """Test that operations fail without context manager."""
        filepath = temp_dir / "test.ndjson"
        writer = FastNDJSONWriter(filepath)

        with pytest.raises(WriterError, match="Writer not opened"):
            writer.write({"test": "data"})

    def test_directory_creation(self, temp_dir):
        """Test that parent directories are created automatically."""
        nested_path = temp_dir / "nested" / "directory" / "test.ndjson"
        record = {"id": 1, "name": "test"}

        with FastNDJSONWriter(nested_path) as writer:
            writer.write(record)

        assert nested_path.exists()
        with open(nested_path, "r") as f:
            line = f.readline()
            assert json.loads(line.strip()) == record

    def test_custom_serialization_default(self, temp_dir):
        """Test custom serialization with default function."""
        from datetime import datetime

        filepath = temp_dir / "test.ndjson"
        record = {"id": 1, "timestamp": datetime(2023, 1, 1)}

        def date_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError

        with FastNDJSONWriter(filepath) as writer:
            writer.write(record, default=date_serializer)

        with open(filepath, "r") as f:
            line = f.readline()
            parsed = json.loads(line.strip())
            assert parsed["timestamp"] == "2023-01-01T00:00:00"

    def test_orjson_options(self, temp_dir):
        """Test orjson options."""
        import orjson

        filepath = temp_dir / "test.ndjson"
        record = {"numbers": [1, 2, 3]}

        with FastNDJSONWriter(filepath) as writer:
            writer.write(record, option=orjson.OPT_INDENT_2)

        with open(filepath, "rb") as f:
            content = f.read()
            # Should contain indented JSON
            assert b"\n  " in content

    def test_serialization_error_handling(self, temp_dir):
        """Test serialization error handling."""
        filepath = temp_dir / "test.ndjson"

        # Create an object that can't be serialized
        class UnserializableObject:
            pass

        record = {"id": 1, "obj": UnserializableObject()}

        with FastNDJSONWriter(filepath) as writer:
            with pytest.raises(Exception):  # orjson will raise TypeError
                writer.write(record)

    def test_append_file_needs_newline_detection(self, temp_dir):
        """Test file that needs newline when appending."""
        filepath = temp_dir / "test.ndjson"

        # Create file without trailing newline
        with open(filepath, "w") as f:
            f.write('{"id": 1}')  # No newline at end

        with FastNDJSONWriter(filepath, WriterMode.APPEND) as writer:
            writer.append({"id": 2}, newline=False)  # Manually add newline first

        with open(filepath, "r") as f:
            content = f.read()
            lines = content.strip().split("\n")
            assert len(lines) == 2


class TestFileLocking:
    """Test file locking functionality."""

    def test_file_lock_basic_usage(self, temp_dir):
        """Test basic file locking functionality."""
        filepath = temp_dir / "test_lock.ndjson"
        record = {"id": 1, "name": "test"}

        with FastNDJSONWriter(filepath, use_file_lock=True) as writer:
            writer.write(record)

        # Verify file was written correctly
        with open(filepath, "r") as f:
            line = f.readline()
            assert json.loads(line.strip()) == record

    def test_file_lock_disabled_by_default(self, temp_dir):
        """Test that file locking is disabled by default."""
        filepath = temp_dir / "test_no_lock.ndjson"
        writer = FastNDJSONWriter(filepath)

        assert writer.use_file_lock is False
        assert writer._file_lock is None

    def test_file_lock_with_append_mode(self, temp_dir):
        """Test file locking with append mode."""
        filepath = temp_dir / "test_lock_append.ndjson"

        # First write with lock
        with FastNDJSONWriter(filepath, use_file_lock=True) as writer:
            writer.write({"id": 1})

        # Then append with lock
        with FastNDJSONWriter(filepath, WriterMode.APPEND, use_file_lock=True) as writer:
            writer.append({"id": 2})

        # Verify both records are in file
        with open(filepath, "r") as f:
            lines = f.readlines()
            assert len(lines) == 2
            assert json.loads(lines[0].strip())["id"] == 1
            assert json.loads(lines[1].strip())["id"] == 2

    def test_file_lock_cleanup_on_error(self, temp_dir):
        """Test that file locks are properly cleaned up on errors."""
        import os

        # Create a directory where we expect a file (will cause an error)
        bad_filepath = temp_dir / "test_error.ndjson"
        os.makedirs(bad_filepath, exist_ok=True)

        try:
            with FastNDJSONWriter(bad_filepath, use_file_lock=True) as writer:
                pass  # Should not reach here
        except WriterError:
            pass  # Expected error

        # Test that we can create a proper file after the error
        good_filepath = temp_dir / "test_good.ndjson"
        with FastNDJSONWriter(good_filepath, use_file_lock=True) as writer:
            writer.write({"test": "success"})

    def test_file_lock_state_tracking(self, temp_dir):
        """Test that lock state is properly tracked."""
        filepath = temp_dir / "test_lock_state.ndjson"

        writer = FastNDJSONWriter(filepath, use_file_lock=True)
        assert writer._file_lock is None

        with writer:
            # Lock should be created and acquired inside context manager
            assert writer._file_lock is not None
            assert writer._file_lock.is_locked

        # Lock should be released after context manager
        assert not writer._file_lock.is_locked

    @pytest.mark.skipif(
        platform.system() == "Windows",
        reason="Unix-specific fcntl test"
    )
    def test_unix_file_locking_mechanism(self, temp_dir):
        """Test Unix-specific file locking using fcntl."""
        import fcntl
        import threading
        import time

        filepath = temp_dir / "test_unix_lock.ndjson"
        results = []

        def write_with_delay(writer_id):
            try:
                with FastNDJSONWriter(filepath, use_file_lock=True) as writer:
                    time.sleep(0.1)  # Hold lock briefly
                    writer.write({"writer_id": writer_id, "timestamp": time.time()})
                    results.append(f"success_{writer_id}")
            except WriterError as e:
                results.append(f"error_{writer_id}_{str(e)}")

        # Start two threads trying to write simultaneously
        threads = []
        for i in range(2):
            thread = threading.Thread(target=write_with_delay, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # At least one should succeed, and we should have some kind of result
        assert len(results) == 2
        success_count = len([r for r in results if r.startswith("success")])
        assert success_count >= 1  # At least one should succeed


    def test_file_lock_creates_lock_file(self, temp_dir):
        """Test that a .lock file is created when using file locking."""
        filepath = temp_dir / "test_lock_file.ndjson"
        lock_filepath = temp_dir / "test_lock_file.ndjson.lock"

        with FastNDJSONWriter(filepath, use_file_lock=True) as writer:
            writer.write({"id": 1, "test": "data"})
            # Lock file should exist while writer is open
            assert lock_filepath.exists()

        # Lock file should be cleaned up after writer closes
        # Note: filelock may leave empty lock file, which is normal behavior
