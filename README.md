# Fast NDJSON Processor

A **high-performance Python library** for processing NDJSON (Newline Delimited JSON) files with **multiprocessing support** and **streaming capabilities**.

## 🚀 **Key Features**

- **⚡ High Performance**: Multiprocessing with chunking for optimal speed
- **🔄 Multiple Processing Modes**: Parallel, Sequential, and Streaming
- **📝 Type Safety**: Full type annotations and mypy support

## 📦 **Installation**

```bash
pip install fast-ndjson-processor
```

## 🎯 **Quick Start**

### Basic Processing

```python
from fast_ndjson_processor import FastNDJSONProcessor

# Create processor
processor = FastNDJSONProcessor(n_workers=4, show_progress=True)

# Define your processing function
def extract_user_data(record):
    return {
        "user_id": record["id"],
        "name": record["name"],
        "email": record.get("email")
    }

# Process the file
results = processor.process_file("users.ndjson", extract_user_data)
print(f"Processed {len(results)} records")
```

### Streaming for Large Files

```python
# Memory-efficient streaming
for record in processor.stream_file("large_file.ndjson"):
    # Process one record at a time
    process_record(record)
```

### Writing NDJSON Files

```python
from fast_ndjson_processor import FastNDJSONWriter

# Write data to NDJSON file
data = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
]

with FastNDJSONWriter("output.ndjson") as writer:
    writer.save(data)
```

## 📖 **Documentation**

### Processing Modes

#### 1. Parallel Mode (Default)
```python
from fast_ndjson_processor import ProcessorMode

results = processor.process_file(
    "data.ndjson", 
    handler_function, 
    mode=ProcessorMode.PARALLEL
)
```

#### 2. Sequential Mode
```python
results = processor.process_file(
    "data.ndjson", 
    handler_function, 
    mode=ProcessorMode.SEQUENTIAL
)
```

#### 3. Streaming Mode
```python
results = processor.process_file(
    "data.ndjson", 
    handler_function, 
    mode=ProcessorMode.STREAMING
)
```

## 🧪 **Testing**

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=fast_ndjson_processor --cov-report=html

# Run specific test categories
pytest -m "not slow"  # Skip slow tests
pytest -m "integration"  # Run only integration tests
```

## 🤝 **Contributing**

Please see [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/fast_ndjson_processor.git
cd fast_ndjson_processor

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e .[dev]

# Run pre-commit hooks
pre-commit install

# Run tests
pytest
```

### Code Quality

```bash
# Format code
black .
isort .

# Lint code
flake8 fast_ndjson_processor tests

# Type checking
mypy fast_ndjson_processor

# Security check
bandit -r fast_ndjson_processor/
```

## 📄 **License**

This project is licensed under GPLv3 - see the [LICENSE](LICENSE) file for details.

## 🙏 **Acknowledgments**

- Inspired by [pushshift/ndjson_processor](https://github.com/pushshift/ndjson_processor) and [pushshift/Parallel-NDJSON-Reader](https://github.com/pushshift/Parallel-NDJSON-Reader).
- Built with [ijl/orjson](https://github.com/ijl/orjson) for fast JSON processing. Thanks [umarbutler/orjsonl](https://github.com/umarbutler/orjsonl) for providing the reference for orjson usage in NDJSON processing.

