"""
Setup script for the NH House of Representatives Video Summarizer package.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="nh_house_of_reps_summarizer",
    version="0.2.0",
    author="Proximile LLC",
    author_email="_",
    description="A package for summarizing NH House of Representatives videos",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/proximile/NH-House-of-Representatives-Committee-Summaries",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=[
        "yt-dlp>=2023.1.1",
        "openai>=1.0.0",
        "beautifulsoup4>=4.10.0",
        "requests>=2.25.0",
        "webvtt-py>=0.4.6",  # Added webvtt-py for better subtitle handling
    ],
    extras_require={
        "whisper": [
            "torch>=2.0.0",
            "transformers>=4.30.0",
            "librosa>=0.10.0",
            "ffmpeg-python>=0.2.0",  # For audio extraction
        ],
        "full": [
            "torch>=2.0.0",
            "transformers>=4.30.0",
            "librosa>=0.10.0",
            "ffmpeg-python>=0.2.0",
            "numpy>=1.22.0",
            "tqdm>=4.64.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "nh_house_of_reps_summarizer=nh_house_of_reps_summarizer.cli:main",
        ],
    },
)