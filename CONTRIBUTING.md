# Contributing to Fast NDJSON Processor

Thank you for your interest in contributing to Fast NDJSON Processor! This document provides guidelines and instructions for contributing to the project.

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Git
- Virtual environment tool (venv, conda, etc.)

### Development Setup

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/friberk/fast-ndjson-processor.git
   cd fast_ndjson_processor
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install development dependencies**
   ```bash
   pip install -e .[dev,progress,yaml,monitoring]
   ```

4. **Install pre-commit hooks**
   ```bash
   pre-commit install
   ```

5. **Run tests to verify setup**
   ```bash
   pytest
   ```

## 🔧 Development Workflow

### 1. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/issue-number-description
```

### 2. Make Your Changes

- Write clean, readable code
- Follow the existing code style
- Add type hints for all functions and methods
- Update docstrings for new/modified functions

### 3. Write Tests

- Add unit tests for new functionality
- Update existing tests if necessary
- Ensure all tests pass: `pytest`
- Aim for >90% code coverage

### 4. Update Documentation

- Update docstrings and type hints
- Update README.md if necessary
- Add examples for new features

### 5. Run Quality Checks

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

# Run all tests
pytest --cov=fast_ndjson_processor
```

### 6. Commit Your Changes

```bash
git add .
git commit -m "Add new feature description"
# or
git commit -m "Resolve issue with..."
```

### 7. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub.

## 📝 Code Style Guidelines

### Python Code Style

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/)
- Use [Black](https://black.readthedocs.io/) for code formatting
- Use [isort](https://pycqa.github.io/isort/) for import sorting
- Line length: 100 characters (configured in pyproject.toml)

### Type Hints

- Add type hints to all function signatures
- Use `typing` module imports for compatibility
- Import types from `typing` when needed

```python
from typing import List, Dict, Optional, Union
from pathlib import Path

def process_data(
    data: List[Dict[str, Any]], 
    output_path: Optional[Path] = None
) -> Dict[str, int]:
    # Implementation
    pass
```

### Documentation

- Use Google-style docstrings
- Include parameter types and descriptions
- Include return value descriptions
- Add examples for complex functions

```python
def process_file(
    filepath: Union[str, Path],
    handler: Callable[[Dict[str, Any]], Any],
    mode: ProcessorMode = ProcessorMode.PARALLEL
) -> Optional[List[Any]]:
    """Process an NDJSON file with the specified handler function.

    Args:
        filepath: Path to the NDJSON file
        handler: Function to process each JSON record
        mode: Processing mode (PARALLEL, SEQUENTIAL, or STREAMING)

    Returns:
        List of processed results if return_results=True, None otherwise

    Raises:
        FileHandlingError: If the file doesn't exist
        ConfigurationError: If an invalid processing mode is specified

    Example:
        >>> processor = FastNDJSONProcessor()
        >>> results = processor.process_file("data.ndjson", lambda x: x["id"])
        >>> print(f"Processed {len(results)} records")
    """
```

## 🧪 Testing Guidelines

### Test Structure

- Place tests in the `tests/` directory
- Mirror the package structure in test files
- Use descriptive test names: `test_process_file_with_invalid_json`

### Test Categories

- **Unit tests**: Test individual functions/methods
- **Integration tests**: Test component interactions
- **Performance tests**: Benchmark critical paths

### Writing Tests

```python
import pytest
from fast_ndjson_processor import FastNDJSONProcessor
from fast_ndjson_processor.exceptions import ConfigurationError

class TestFastNDJSONProcessor:
    def test_init_with_valid_params(self):
        """Test processor initialization with valid parameters."""
        processor = FastNDJSONProcessor(n_workers=4, chunk_size=1024)
        assert processor.n_workers == 4
        assert processor.chunk_size == 1024

    def test_init_with_invalid_workers(self):
        """Test processor initialization with invalid n_workers."""
        with pytest.raises(ConfigurationError, match="n_workers must be positive"):
            FastNDJSONProcessor(n_workers=0)

    def test_process_file_basic(self, create_ndjson_file, mock_handler):
        """Test basic file processing."""
        processor = FastNDJSONProcessor()
        filepath = create_ndjson_file()
        
        results = processor.process_file(filepath, mock_handler)
        
        assert len(results) == 5
        assert all("processed_id" in result for result in results)
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=fast_ndjson_processor --cov-report=html

# Run specific test file
pytest tests/test_processor.py

# Run specific test
pytest tests/test_processor.py::TestFastNDJSONProcessor::test_init_with_valid_params

# Run tests with markers
pytest -m "not slow"  # Skip slow tests
pytest -m "integration"  # Run only integration tests
```

## 📋 Pull Request Guidelines

### Before Submitting

- [ ] All tests pass
- [ ] Code coverage is maintained or improved
- [ ] Code is formatted with Black and isort
- [ ] Type checking passes with mypy
- [ ] Documentation is updated

## 🐛 Reporting Issues

### Bug Reports

Use the GitHub issue template for bug reports. Include:

- Python version
- Operating system
- Library version
- Minimal reproduction example
- Expected vs actual behavior
- Error messages/stack traces

### Feature Requests

Use GitHub Discussions for feature requests. Include:

- Use case description
- Proposed API design
- Alternative solutions considered
- Implementation complexity estimate

## 🏗️ Architecture Guidelines

### Package Structure

```
fast_ndjson_processor/
├── __init__.py          # Public API exports
├── fast_ndjson_processor.py  # Main processor class
├── fast_ndjson_writer.py     # Writer class
├── exceptions.py        # Custom exceptions
└── *.py               # Other modules
```

### Design Principles

- **Single Responsibility**: Each class/function has one clear purpose
- **Open/Closed**: Open for extension, closed for modification
- **Dependency Inversion**: Depend on abstractions, not concretions
- **Type Safety**: Comprehensive type hints throughout
- **Error Handling**: Explicit error handling with custom exceptions

### Performance Considerations

- Use multiprocessing for CPU-bound tasks
- Implement streaming for memory efficiency
- Profile critical paths
- Benchmark against alternatives

## 📚 Documentation

### API Documentation

- All public APIs must have comprehensive docstrings
- Include examples in docstrings where helpful
- Document exceptions that can be raised

### User Documentation

- Keep README.md up-to-date
- Add examples for new features
- Update configuration documentation

## 🔒 Security

### Security Guidelines

- Sanitize user inputs
- Use safe evaluation for expressions
- Validate file paths
- Report security issues privately

### Security Review Process

1. Run security checks: `bandit -r fast_ndjson_processor/`
2. Review dependency vulnerabilities: `safety check`
3. Manual security review for sensitive changes

## 📦 Release Process

### Version Numbering

Follow [Semantic Versioning](https://semver.org/):
- MAJOR: Breaking changes
- MINOR: New features (backward compatible)
- PATCH: Bug fixes (backward compatible)

### Release Checklist

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Run full test suite
4. Create release PR
5. Tag release after merge
6. GitHub Actions will handle PyPI deployment

## 🤝 Code of Conduct

This project follows the [Contributor Covenant](https://www.contributor-covenant.org/version/2/0/code_of_conduct/) Code of Conduct.

## 💬 Getting Help

- **Documentation**: Check README.md and docstrings first
- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and general discussion

## 🙏 Recognition

Contributors will be recognized in:
- CONTRIBUTORS.md file
- GitHub contributors graph
- Release notes for significant contributions

Thank you for contributing to Fast NDJSON Processor! 🚀
