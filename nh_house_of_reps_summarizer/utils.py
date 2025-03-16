"""
Utility functions for the NH House of Representatives Video Summarizer.
"""

import os
import re
import logging
import tempfile
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def setup_logging(level=logging.INFO, log_file=None):
    """
    Set up logging configuration.

    Args:
        level (int): Logging level
        log_file (str, optional): Path to log file
    """
    handlers = []

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    handlers.append(console_handler)

    # File handler (if specified)
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        handlers.append(file_handler)

    # Configure root logger
    logging.basicConfig(
        level=level,
        handlers=handlers
    )


def format_duration(seconds):
    """
    Format duration in seconds to a human-readable string.

    Args:
        seconds (int): Duration in seconds

    Returns:
        str: Formatted duration string
    """
    if not seconds:
        return "Unknown duration"

    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)

    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"


def format_date(date_str):
    """
    Format a YYYYMMDD date string to a readable format.

    Args:
        date_str (str): Date string in YYYYMMDD format

    Returns:
        str: Formatted date string
    """
    if not date_str or len(date_str) != 8:
        return "Unknown date"

    try:
        year = date_str[0:4]
        month = date_str[4:6]
        day = date_str[6:8]

        return f"{year}-{month}-{day}"
    except:
        return date_str


def chunk_text(text, max_chunk_size, overlap=100):
    """
    Split text into chunks of more or less uniform size with optional overlap.
    This version *does not* look for natural breakpoints (periods/spaces),
    so chunking will be more uniform.

    Args:
        text (str): The text to split
        max_chunk_size (int): Maximum size of each chunk (in characters)
        overlap (int): Number of characters of overlap between chunks

    Returns:
        list: List of text chunks
    """
    if len(text) <= max_chunk_size:
        return [text]

    total_length = len(text)
    # Estimate how many chunks we need
    num_chunks = (total_length // (max_chunk_size - overlap))
    if total_length % (max_chunk_size - overlap) != 0:
        num_chunks += 1

    # We’ll still compute a "target" chunk size for an even distribution,
    # though we will not look for sentence breaks. We simply slice.
    chunk_target_size = min(max_chunk_size, (total_length // num_chunks + overlap))

    logger.info(
        f"Splitting text of length {total_length} into ~{num_chunks} chunks of size ~{chunk_target_size} with overlap={overlap}"
    )

    chunks = []
    position = 0

    while position < total_length:
        chunk_end = min(position + chunk_target_size, total_length)

        # Extract the chunk exactly
        current_chunk = text[position:chunk_end]
        chunks.append(current_chunk)
        logger.info(f"Created chunk {len(chunks)} with {len(current_chunk)} characters")

        # Move for next chunk, applying overlap if not at the very end
        if chunk_end < total_length:
            position = chunk_end - overlap
        else:
            position = chunk_end

    logger.info(f"Finished splitting text into {len(chunks)} chunks.")
    return chunks


def get_temp_file(suffix=None):
    """
    Get a temporary file path.

    Args:
        suffix (str, optional): File suffix/extension

    Returns:
        str: Path to a temporary file
    """
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    return path


def clean_filename(filename):
    """
    Clean a string so it can be safely used as a filename.

    Args:
        filename (str): Original filename

    Returns:
        str: Cleaned filename
    """
    # Remove or replace characters not allowed in filenames
    cleaned = re.sub(r'[\\/*?:"<>|]', '_', filename)
    # Strip leading or trailing spaces
    cleaned = cleaned.strip()
    return cleaned


def is_transcript_format_supported(file_path):
    """
    Check if a transcript file format is supported by webvtt-py.

    Args:
        file_path (str): Path to the transcript file

    Returns:
        bool: True if the format is supported, False otherwise
    """
    if not os.path.exists(file_path):
        return False

    _, ext = os.path.splitext(file_path)
    ext = ext.lower().lstrip('.')
    return ext in ['vtt', 'srt', 'sbv']



def format_duration(seconds):
    """
    Format duration in seconds to a human-readable string.
    
    Args:
        seconds (int): Duration in seconds
        
    Returns:
        str: Formatted duration string
    """
    if not seconds:
        return "Unknown duration"
        
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def format_date(date_str):
    """
    Format a YYYYMMDD date string to a readable format.
    
    Args:
        date_str (str): Date string in YYYYMMDD format
        
    Returns:
        str: Formatted date string
    """
    if not date_str or len(date_str) != 8:
        return "Unknown date"
        
    try:
        year = date_str[0:4]
        month = date_str[4:6]
        day = date_str[6:8]
        
        return f"{year}-{month}-{day}"
    except:
        return date_str

def date_to_period(date_str, period='day'):
    """
    Convert a date string to a period string.
    
    Args:
        date_str (str): Date string in YYYYMMDD format
        period (str): Time period (day, week, month, year)
        
    Returns:
        str: Period string
    """
    if not date_str or len(date_str) != 8:
        return "unknown"
    
    try:
        year = date_str[0:4]
        month = date_str[4:6]
        day = date_str[6:8]
        
        if period == 'day':
            return f"{year}-{month}-{day}"
        elif period == 'week':
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            # Get the week number and year
            week_num = date_obj.isocalendar()[1]
            week_year = date_obj.isocalendar()[0]
            return f"{week_year}-W{week_num:02d}"
        elif period == 'month':
            return f"{year}-{month}"
        elif period == 'year':
            return year
        else:
            return f"{year}-{month}-{day}"
    except:
        return "unknown"

def get_file_extension(file_path):
    """
    Get the extension of a file.
    
    Args:
        file_path (str): Path to the file
        
    Returns:
        str: File extension without the dot
    """
    _, ext = os.path.splitext(file_path)
    return ext.lower().lstrip('.')