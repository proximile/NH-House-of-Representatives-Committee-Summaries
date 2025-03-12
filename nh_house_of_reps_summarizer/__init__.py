"""
NH House of Representatives Video Summarizer

A Python package that downloads, processes, and summarizes videos from the 
NH House of Representatives YouTube channel using yt-dlp and the Hyperbolic API.
Automatically exports summaries to text files after generation.
"""

__version__ = "0.2.0"

from .main import NHVideoProcessor
from .video_downloader import VideoDownloader
from .database import DatabaseManager
from .summarizer import TranscriptSummarizer, AsyncTranscriptSummarizer
from .audio_transcriber import AudioTranscriber
from .utils import setup_logging, format_date, format_duration

__all__ = [
    'NHVideoProcessor',
    'VideoDownloader', 
    'DatabaseManager',
    'TranscriptSummarizer',
    'AsyncTranscriptSummarizer',
    'AudioTranscriber',
    'setup_logging',
    'format_date',
    'format_duration'
]