#!/usr/bin/env python3
"""
Setup script for Azure Uploader.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

# Read requirements from parent directory
requirements_file = Path(__file__).parent.parent / "requirements.txt"
requirements = []
if requirements_file.exists():
    # Filter to only include the dependencies needed for azure-uploader
    all_requirements = requirements_file.read_text().strip().split('\n')
    # Include only non-comment, non-empty lines that are actual dependencies
    requirements = [
        line.strip() for line in all_requirements
        if line.strip() and not line.strip().startswith('#')
    ]

setup(
    name="azure-uploader",
    version="1.0.0",
    description="A simple, robust Python utility for uploading files and directories to Azure Blob Storage",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/yourusername/azure-uploader",
    packages=find_packages(),
    py_modules=["azure_uploader", "config", "cli"],
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "azure-uploader=cli:cli",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Archiving :: Backup",
    ],
    python_requires=">=3.8",
    keywords="azure blob storage upload backup cloud",
    project_urls={
        "Bug Reports": "https://github.com/yourusername/azure-uploader/issues",
        "Source": "https://github.com/yourusername/azure-uploader",
    },
)
