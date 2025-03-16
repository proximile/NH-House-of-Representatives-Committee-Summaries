"""
Command-line interface for the NH House of Representatives Video Summarizer.
With Whisper transcription options.
"""

import os
import sys
import argparse
import logging
import textwrap
from datetime import datetime

def get_api_key(args):
    """
    Get the API key from arguments or environment.
    
    Args:
        args: Command-line arguments
        
    Returns:
        str: API key
    """
    api_key = None
    
    # Try to get API key from command-line arguments
    if args.api_key:
        api_key = args.api_key
    elif args.api_key_file:
        try:
            with open(args.api_key_file, 'r') as f:
                api_key = f.read().strip()
        except Exception as e:
            print(f"Error reading API key file: {e}")
    
    # Fall back to environment variable
    if not api_key:
        api_key = os.getenv("HYPERBOLIC_API_KEY")
        
    return api_key

def main():
    """Main entry point for the command-line interface."""
    parser = argparse.ArgumentParser(description="NH House of Representatives Video Summarizer")
    
    # Common arguments
    parser.add_argument("--api-key", help="Hyperbolic API key")
    parser.add_argument("--api-key-file", help="Path to file containing Hyperbolic API key")
    parser.add_argument("--download-dir", default="downloads", help="Directory for downloading transcripts")
    parser.add_argument("--output-dir", default="summaries", help="Directory for saving summary files")
    parser.add_argument("--db-path", default="nh_videos.db", help="Path to the SQLite database")
    parser.add_argument("--log-file", help="Path to log file")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    # Whisper transcription options
    whisper_group = parser.add_argument_group('Whisper Transcription Options')
    whisper_group.add_argument("--whisper", action="store_true", 
                             help="Enable Whisper transcription as fallback when YouTube captions aren't available")
    whisper_group.add_argument("--no-whisper", action="store_true", 
                             help="Disable Whisper transcription fallback")
    whisper_group.add_argument("--whisper-model", default="openai/whisper-large-v3-turbo", 
                             help="Whisper model to use (default: openai/whisper-large-v3-turbo)")
    whisper_group.add_argument("--whisper-chunk", type=int, default=30, 
                             help="Chunk length in seconds for Whisper processing (default: 30)")
    whisper_group.add_argument("--whisper-batch", type=int, default=16, 
                             help="Batch size for Whisper processing (default: 16)")
    whisper_group.add_argument("--force-whisper", action="store_true", 
                             help="Force using Whisper even if YouTube captions are available")
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Fetch command
    fetch_parser = subparsers.add_parser("fetch", help="Fetch videos from the channel")
    fetch_parser.add_argument("--limit", type=int, help="Maximum number of videos to fetch")
    
    # Transcripts command
    transcripts_parser = subparsers.add_parser("transcripts", help="Download transcripts for videos")
    transcripts_parser.add_argument("--limit", type=int, help="Maximum number of videos to download transcripts for")
    transcripts_parser.add_argument("--format", choices=['vtt', 'srt', 'sbv'], default='vtt', 
                                  help="Format to download transcripts in (default: vtt)")
    transcripts_parser.add_argument("--force-whisper", action="store_true", 
                                  help="Force using Whisper for transcription even if YouTube captions are available")
    
    # Process command
    process_parser = subparsers.add_parser("process", help="Process videos and generate summaries with immediate file output")
    process_parser.add_argument("--limit", type=int, default=5, help="Maximum number of videos to process")
    process_parser.add_argument("--delay", type=int, default=2, help="Delay between API calls in seconds")
    process_parser.add_argument("--flat", action="store_true", help="Use flat directory structure (don't organize by date)")
    process_parser.add_argument("--filename-template", default="{id}_{title}.txt",
                             help="Template for filenames (variables: {id}, {title}, {date})")
    process_parser.add_argument("--overwrite", action="store_true", help="Overwrite existing summary files")
    process_parser.add_argument("--async", dest="async_mode", action="store_true", help="Use async mode for processing")
    
    # Pipeline command
    pipeline_parser = subparsers.add_parser("pipeline", help="Run the full pipeline")
    pipeline_parser.add_argument("--fetch-limit", type=int, default=50, help="Maximum number of videos to fetch")
    pipeline_parser.add_argument("--process-limit", type=int, default=10, help="Maximum number of videos to process")
    pipeline_parser.add_argument("--delay", type=int, default=2, help="Delay between API calls in seconds")
    pipeline_parser.add_argument("--format", choices=['vtt', 'srt', 'sbv'], default='vtt',
                               help="Format to download transcripts in (default: vtt)")
    pipeline_parser.add_argument("--flat", action="store_true", help="Use flat directory structure (don't organize by date)")
    pipeline_parser.add_argument("--filename-template", default="{id}_{title}.txt",
                             help="Template for filenames (variables: {id}, {title}, {date})")
    pipeline_parser.add_argument("--overwrite", action="store_true", help="Overwrite existing summary files")
    pipeline_parser.add_argument("--async", dest="async_mode", action="store_true", help="Use async mode for processing")
    pipeline_parser.add_argument("--force-whisper", action="store_true", 
                               help="Force using Whisper for transcription even if YouTube captions are available")
    
    # Search command
    search_parser = subparsers.add_parser("search", help="Search videos in the database")
    search_parser.add_argument("query", nargs="?", help="Search query for titles or summaries")
    search_parser.add_argument("--limit", type=int, default=20, help="Maximum number of results to return")
    
    # Info command
    info_parser = subparsers.add_parser("info", help="Show information about a video")
    info_parser.add_argument("video_id", nargs="?", help="Video ID")
    
    # Extract command
    extract_parser = subparsers.add_parser("extract", help="Extract a summary to a file")
    extract_parser.add_argument("video_id", help="Video ID")
    extract_parser.add_argument("--output", "-o", help="Output file path")
    
    # Convert command
    convert_parser = subparsers.add_parser("convert", help="Convert a transcript to a different format")
    convert_parser.add_argument("video_id", help="Video ID")
    convert_parser.add_argument("--format", "-f", required=True, choices=['vtt', 'srt', 'sbv'],
                              help="Target format (vtt, srt, sbv)")
    
    # Export command - for exporting all summaries
    export_parser = subparsers.add_parser("export", help="Export summaries to text files")
    export_parser.add_argument("--output-dir", default="summaries", help="Directory to save summaries in")
    export_parser.add_argument("--flat", action="store_true", help="Use flat directory structure (don't organize by date)")
    export_parser.add_argument("--limit", type=int, help="Maximum number of summaries to export")
    export_parser.add_argument("--filename-template", default="{id}_{title}.txt",
                             help="Template for filenames (variables: {id}, {title}, {date})")
    export_parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    export_parser.add_argument("--query", help="Only export summaries matching this search query")
    export_parser.add_argument("--list-files", action="store_true", help="List exported file paths")
    
    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show statistics about videos and summaries")
    
    # Whisper-specific commands
    whisper_cmd = subparsers.add_parser("whisper", help="Commands related to Whisper transcription")
    whisper_subparsers = whisper_cmd.add_subparsers(dest="whisper_command", help="Whisper command to execute")
    
    # Whisper transcribe command
    whisper_transcribe = whisper_subparsers.add_parser("transcribe", help="Transcribe a video using Whisper")
    whisper_transcribe.add_argument("video_id", help="Video ID to transcribe")
    whisper_transcribe.add_argument("--format", "-f", choices=['vtt', 'srt', 'sbv'], default='vtt',
                                 help="Output format (default: vtt)")
    whisper_transcribe.add_argument("--model", default="openai/whisper-large-v3-turbo", 
                                 help="Whisper model to use")
    whisper_transcribe.add_argument("--force", action="store_true", 
                                 help="Force transcription even if transcript already exists")
    
    # Check Whisper installation
    whisper_check = whisper_subparsers.add_parser("check", help="Check Whisper installation")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Set up logging
    from utils import setup_logging
    
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(level=log_level, log_file=args.log_file)
    
    # Get API key
    api_key = get_api_key(args)
    if not api_key and args.command in ['process', 'pipeline']:
        print("Error: Hyperbolic API key is required for summarization")
        print("Set the HYPERBOLIC_API_KEY environment variable or use --api-key / --api-key-file")
        sys.exit(1)
    
    # Determine whether to use Whisper
    use_whisper = True  # Default to enabled 
    
    if args.no_whisper:
        use_whisper = False
    elif args.whisper:
        use_whisper = True
        
    if args.force_whisper and not use_whisper:
        print("Warning: --force-whisper was specified but Whisper is disabled (--no-whisper)")
        print("Enabling Whisper for this command")
        use_whisper = True
        
    # Import main processor here to avoid circular imports
    from main import NHVideoProcessor
    
    # Create processor
    processor = NHVideoProcessor(
        api_key=api_key,
        download_dir=args.download_dir,
        db_path=args.db_path,
        output_dir=args.output_dir,
        use_whisper_fallback=use_whisper,
        whisper_model_id=args.whisper_model if hasattr(args, 'whisper_model') else "openai/whisper-large-v3-turbo",
        whisper_chunk_length=args.whisper_chunk if hasattr(args, 'whisper_chunk') else 30,
        whisper_batch_size=args.whisper_batch if hasattr(args, 'whisper_batch') else 16,
        force_whisper=args.force_whisper if hasattr(args, 'force_whisper') else False
    )
    
    # Import command handlers here
    from cli_commands import (
        command_fetch, command_transcripts, command_process, command_pipeline,
        command_search, command_info, command_extract, command_convert,
        command_export, command_stats, command_whisper_transcribe, command_whisper_check
    )
    
    # Execute command
    if args.command == "fetch":
        command_fetch(args, processor)
    elif args.command == "transcripts":
        command_transcripts(args, processor)
    elif args.command == "process":
        command_process(args, processor)
    elif args.command == "pipeline":
        command_pipeline(args, processor)
    elif args.command == "search":
        command_search(args, processor)
    elif args.command == "info":
        command_info(args, processor)
    elif args.command == "extract":
        command_extract(args, processor)
    elif args.command == "convert":
        command_convert(args, processor)
    elif args.command == "export":
        command_export(args, processor)
    elif args.command == "stats":
        command_stats(args, processor)
    elif args.command == "whisper":
        if args.whisper_command == "transcribe":
            command_whisper_transcribe(args, processor)
        elif args.whisper_command == "check":
            command_whisper_check(args)
        else:
            parser.print_help()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()