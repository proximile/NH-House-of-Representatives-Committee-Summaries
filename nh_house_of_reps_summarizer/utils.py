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
    Split text into chunks of max_chunk_size with some overlap.
    
    Args:
        text (str): Text to split
        max_chunk_size (int): Maximum size of each chunk
        overlap (int): Number of characters to overlap between chunks
        
    Returns:
        list: List of text chunks
    """
    if len(text) <= max_chunk_size:
        return [text]
    
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + max_chunk_size
        
        if end >= len(text):
            chunks.append(text[start:])
            break
        
        # Try to find a sentence break
        sentence_break = text.rfind('. ', start, end)
        if sentence_break != -1:
            end = sentence_break + 2  # Include the period and space
        else:
            # If no sentence break, try to find a space
            space = text.rfind(' ', start, end)
            if space != -1:
                end = space + 1  # Include the space
        
        chunks.append(text[start:end])
        start = end - overlap  # overlap with previous chunk
    
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
    os.close(fd)  # Close the file descriptor
    return path

def clean_filename(filename):
    """
    Clean a filename by removing invalid characters.
    
    Args:
        filename (str): Original filename
        
    Returns:
        str: Cleaned filename
    """
    # Replace invalid characters with underscores
    return re.sub(r'[\\/*?:"<>|]', '_', filename)

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

def is_transcript_format_supported(file_path):
    """
    Check if a file has a supported transcript format extension.
    
    Args:
        file_path (str): Path to the file
        
    Returns:
        bool: True if the format is supported, False otherwise
    """
    ext = get_file_extension(file_path)
    return ext in ['vtt', 'srt', 'sbv']