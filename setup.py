"""
Setup script for Parquet File Generator
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="parquet-file-generator",
    version="1.0.0",
    author="markcurtis1970",
    description="A configurable system for generating multiple parquet files with custom schemas",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/markcurtis1970/WD40-parquet",
    packages=find_packages(),
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
        "Topic :: Software Development :: Testing",
        "Topic :: Database",
        "Topic :: Scientific/Engineering",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "parquet-generator=cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["examples/*.yaml", "*.md", "*.txt"],
    },
)
