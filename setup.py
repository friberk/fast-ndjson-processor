"""Setup script for fast_ndjson_processor package."""

from pathlib import Path

from setuptools import find_packages, setup

# Read the contents of README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="fast_ndjson_processor",
    version="1.0.0",
    author="Berk Çakar",
    description="A high-performance Python library for processing NDJSON files with multiprocessing support",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/friberk/fast-ndjson-processor",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Text Processing",
        "Topic :: Utilities",
        "Typing :: Typed",
    ],
    python_requires=">=3.8",
    install_requires=[
        "orjson>=3.8.0",
        "tqdm>=4.64.0",
        "filelock>=3.12.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-benchmark>=4.0.0",
            "mypy>=1.0.0",
            "black>=22.0.0",
            "isort>=5.10.0",
            "flake8-pyproject>=1.2.3",
            "bandit>=1.7.0",
            "safety>=2.0.0",
            "pre-commit>=3.0.0",
        ],
    },
    include_package_data=True,
    package_data={
        "fast_ndjson_processor": ["py.typed"],
    },
    keywords="ndjson json processing multiprocessing streaming performance",
    project_urls={
        "Bug Reports": "https://github.com/friberk/fast-ndjson-processor/issues",
        "Source": "https://github.com/friberk/fast-ndjson-processor",
    },
)
