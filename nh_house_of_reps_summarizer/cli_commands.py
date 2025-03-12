"""
Command implementations for the NH House of Representatives Video Summarizer CLI.
With Whisper transcription support.
"""

import os
import sys
import textwrap
import logging
from datetime import datetime
from .utils import setup_logging, format_duration, format_date, is_transcript_format_supported

logger = logging.getLogger(__name__)

def print_video_info(video, show_summary=True):
    """
    Print information about a video.
    
    Args:
        video (dict): Video data
        show_summary (bool): Whether to show the summary
    """
    print("=" * 80)
    print(f"Title: {video['title']}")
    print(f"ID: {video['id']}")
    print(f"URL: {video['url']}")
    print(f"Upload Date: {format_date(video['upload_date'])}")
    
    if video.get('duration'):
        print(f"Duration: {format_duration(video['duration'])}")
        
    if video.get('view_count'):
        print(f"Views: {video['view_count']}")
    
    if video.get('transcript_path'):
        print(f"Transcript: {video['transcript_path']}")
        
    print("-" * 80)
    
    if show_summary and video.get('summary'):
        print("Summary:")
        print()
        print(textwrap.fill(video['summary'], width=80))
        print()
    elif show_summary:
        print("No summary available")
        
    print("=" * 80)

def command_fetch(args, processor):
    """
    Handle the fetch command.
    
    Args:
        args: Command-line arguments
        processor: NHVideoProcessor instance
    """
    print(f"Fetching videos from NH House of Representatives channel...")
    count = processor.fetch_channel_videos(limit=args.limit)
    print(f"Fetched {count} videos")

def command_transcripts(args, processor):
    """
    Handle the transcripts command.
    
    Args:
        args: Command-line arguments
        processor: NHVideoProcessor instance
    """
    format_type = args.format.lower() if args.format else 'vtt'
    
    if not is_transcript_format_supported(f"dummy.{format_type}"):
        print(f"Error: Unsupported transcript format: {format_type}")
        print("Supported formats: vtt, srt, sbv")
        return
    
    # Set force_whisper in downloader if specified
    if hasattr(args, 'force_whisper') and args.force_whisper:
        processor.downloader.force_whisper = True
        print(f"Using Whisper for transcription (forced)")
    
    print(f"Downloading transcripts for videos in {format_type} format...")
    count = processor.download_transcripts(limit=args.limit, format_type=format_type)
    print(f"Downloaded {count} transcripts")

def command_process(args, processor):
    """
    Handle the process command.
    
    Args:
        args: Command-line arguments
        processor: NHVideoProcessor instance
    """
    print(f"Processing videos and generating summaries...")
    count = processor.process_videos(
        limit=args.limit, 
        delay=args.delay,
        organize_by_date=not args.flat,
        filename_template=args.filename_template,
        overwrite=args.overwrite,
        async_mode=args.async_mode
    )
    print(f"Processed {count} videos")
    if count > 0:
        print(f"Summary files have been saved to the output directory")

def command_pipeline(args, processor):
    """
    Handle the pipeline command.
    
    Args:
        args: Command-line arguments
        processor: NHVideoProcessor instance
    """
    format_type = args.format.lower() if args.format else 'vtt'
    
    if not is_transcript_format_supported(f"dummy.{format_type}"):
        print(f"Error: Unsupported transcript format: {format_type}")
        print("Supported formats: vtt, srt, sbv")
        return
    
    # Set force_whisper in downloader if specified
    if hasattr(args, 'force_whisper') and args.force_whisper:
        processor.downloader.force_whisper = True
        print(f"Using Whisper for transcription (forced)")
    
    print(f"Running full pipeline with {format_type} format transcripts...")
    fetch_count, transcript_count, process_count = processor.run_pipeline(
        fetch_limit=args.fetch_limit,
        process_limit=args.process_limit,
        delay=args.delay,
        transcript_format=format_type,
        organize_by_date=not args.flat,
        filename_template=args.filename_template,
        overwrite=args.overwrite,
        async_mode=args.async_mode
    )
    print(f"Pipeline completed:")
    print(f"  - Fetched {fetch_count} videos")
    print(f"  - Downloaded {transcript_count} transcripts")
    print(f"  - Processed {process_count} videos")
    if process_count > 0:
        print(f"  - Summary files have been saved to the output directory")

def command_search(args, processor):
    """
    Handle the search command.
    
    Args:
        args: Command-line arguments
        processor: NHVideoProcessor instance
    """
    if not args.query:
        print("Please provide a search query")
        return
        
    print(f"Searching for videos matching '{args.query}'...")
    videos = processor.search_videos(args.query, limit=args.limit)
    
    if not videos:
        print("No videos found matching your query")
        return
        
    print(f"Found {len(videos)} videos:")
    for i, video in enumerate(videos):
        print(f"{i+1}. {video['title']} ({format_date(video['upload_date'])})")
        
    while True:
        try:
            choice = input("\nEnter number to view details (or 'q' to quit): ")
            if choice.lower() == 'q':
                break
                
            index = int(choice) - 1
            if 0 <= index < len(videos):
                print_video_info(videos[index])
            else:
                print("Invalid selection")
        except ValueError:
            print("Please enter a number or 'q'")
        except KeyboardInterrupt:
            break

def command_info(args, processor):
    """
    Handle the info command.
    
    Args:
        args: Command-line arguments
        processor: NHVideoProcessor instance
    """
    if not args.video_id:
        print("Please provide a video ID")
        return
        
    video = processor.get_video_summary(args.video_id)
    
    if not video:
        print(f"Video not found: {args.video_id}")
        return
        
    print_video_info(video)

def command_extract(args, processor):
    """
    Handle the extract command.
    
    Args:
        args: Command-line arguments
        processor: NHVideoProcessor instance
    """
    if not args.video_id:
        print("Please provide a video ID")
        return
        
    video = processor.get_video_summary(args.video_id)
    
    if not video:
        print(f"Video not found: {args.video_id}")
        return
        
    if not video.get('summary'):
        print(f"No summary available for video: {args.video_id}")
        return
        
    output_file = args.output
    if not output_file:
        output_file = f"{args.video_id}_summary.txt"
        
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"Title: {video['title']}\n")
            f.write(f"URL: {video['url']}\n")
            f.write(f"Upload Date: {format_date(video['upload_date'])}\n\n")
            f.write(video['summary'])
            
        print(f"Summary saved to {output_file}")
    except Exception as e:
        print(f"Error saving summary: {e}")

def command_convert(args, processor):
    """
    Handle the convert command.
    
    Args:
        args: Command-line arguments
        processor: NHVideoProcessor instance
    """
    if not args.video_id:
        print("Please provide a video ID")
        return
        
    if not args.format:
        print("Please specify a target format (vtt, srt)")
        return
        
    format_type = args.format.lower()
    if not is_transcript_format_supported(f"dummy.{format_type}"):
        print(f"Error: Unsupported transcript format: {format_type}")
        print("Supported formats: vtt, srt, sbv")
        return
        
    print(f"Converting transcript for video {args.video_id} to {format_type} format...")
    
    converted_path = processor.convert_transcript(args.video_id, format_type)
    
    if converted_path:
        print(f"Transcript converted and saved to: {converted_path}")
    else:
        print(f"Failed to convert transcript for video: {args.video_id}")

def command_export(args, processor):
    """
    Handle the export command to save summaries to text files.
    
    Args:
        args: Command-line arguments
        processor: NHVideoProcessor instance
    """
    output_dir = args.output_dir
    organize = not args.flat
    
    print(f"Exporting summaries to {output_dir}")
    if organize:
        print("Organizing by date (year/month folders)")
    else:
        print("Using flat directory structure")
        
    if args.query:
        print(f"Filtering by search query: '{args.query}'")
        
    count, exported_files = processor.export_summaries(
        output_dir=output_dir,
        organize_by_date=organize,
        limit=args.limit,
        filename_template=args.filename_template,
        overwrite=args.overwrite,
        query=args.query
    )
    
    if count == 0:
        print("No summaries were exported")
    else:
        print(f"Exported {count} summaries to {output_dir}")
        
        if args.list_files and exported_files:
            print("\nExported files:")
            for file_path in exported_files:
                print(f"  - {file_path}")

def command_stats(args, processor):
    """
    Display statistics about the videos and summaries.
    
    Args:
        args: Command-line arguments
        processor: NHVideoProcessor instance
    """
    print("NH House of Representatives Video Statistics")
    print("=" * 50)
    
    stats = processor.db.get_summary_stats()
    
    print(f"Total videos in database: {stats['total_videos']}")
    print(f"Videos with transcripts: {stats['total_transcripts']}")
    print(f"Videos with summaries: {stats['total_summaries']}")
    print(f"Videos pending processing: {stats['pending_processing']}")
    
    if stats.get('months'):
        print("\nVideos by month:")
        for month, count in stats['months'].items():
            if len(month) == 6:
                year = month[:4]
                month_num = month[4:6]
                print(f"  {year}-{month_num}: {count} videos")

def command_whisper_transcribe(args, processor):
    """
    Handle the whisper transcribe command.
    
    Args:
        args: Command-line arguments
        processor: NHVideoProcessor instance
    """
    if not args.video_id:
        print("Please provide a video ID")
        return
        
    format_type = args.format.lower() if args.format else 'vtt'
    
    if not is_transcript_format_supported(f"dummy.{format_type}"):
        print(f"Error: Unsupported transcript format: {format_type}")
        print("Supported formats: vtt, srt, sbv")
        return
        
    # Check if transcription already exists
    existing_path = os.path.join(processor.downloader.transcript_dir, f"{args.video_id}.{format_type}")
    if os.path.exists(existing_path) and not args.force:
        print(f"Transcript already exists at {existing_path}")
        print("Use --force to overwrite")
        return
        
    # Set custom model if specified
    if args.model:
        original_model = processor.downloader.transcriber.model_id
        processor.downloader.transcriber.model_id = args.model
        print(f"Using specified Whisper model: {args.model}")
        
    print(f"Transcribing video {args.video_id} using Whisper...")
    
    # Force using Whisper regardless of YouTube captions
    processor.downloader.force_whisper = True
    transcript_path = processor.downloader.transcribe_with_whisper(args.video_id, format_type)
    
    # Reset model if it was changed
    if args.model:
        processor.downloader.transcriber.model_id = original_model
    
    if transcript_path:
        print(f"Transcription successful. Saved to: {transcript_path}")
    else:
        print("Transcription failed")

def command_whisper_check(args):
    """
    Check if Whisper dependencies are installed.
    
    Args:
        args: Command-line arguments
    """
    print("Checking Whisper dependencies...")
    
    missing = []
    
    # Check for torch
    try:
        import torch
        print(f"✓ PyTorch installed: {torch.__version__}")
        
        if torch.cuda.is_available():
            print(f"✓ CUDA available: {torch.version.cuda}")
            print(f"✓ GPU: {torch.cuda.get_device_name(0)}")
        else:
            print("⚠ CUDA not available. Transcription will be slow on CPU.")
    except ImportError:
        print("✗ PyTorch not installed")
        missing.append("torch")
    
    # Check for transformers
    try:
        import transformers
        print(f"✓ Transformers installed: {transformers.__version__}")
    except ImportError:
        print("✗ Transformers not installed")
        missing.append("transformers")
        
    # Check for librosa
    try:
        import librosa
        print(f"✓ Librosa installed: {librosa.__version__}")
    except ImportError:
        print("✗ Librosa not installed")
        missing.append("librosa")
        
    # Check for ffmpeg
    try:
        import subprocess
        result = subprocess.run(['ffmpeg', '-version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            version_line = result.stdout.split('\n')[0]
            print(f"✓ FFmpeg installed: {version_line}")
        else:
            print("✗ FFmpeg not found")
            missing.append("ffmpeg")
    except Exception:
        print("✗ FFmpeg not found or error checking version")
        missing.append("ffmpeg")
        
    # Print installation instructions if dependencies are missing
    if missing:
        print("\nMissing dependencies detected. Install them with:")
        print("pip install " + " ".join(missing))
        
        if "ffmpeg" in missing:
            print("\nFFmpeg installation:")
            print("For Windows: Download from https://ffmpeg.org/download.html")
            print("For macOS: brew install ffmpeg")
            print("For Linux: apt-get install ffmpeg (Ubuntu/Debian) or equivalent")
            
        return False
    else:
        print("\nAll Whisper dependencies are installed!")
        return True