"""
Video and transcript downloader for NH House of Representatives YouTube videos.
With Whisper speech-to-text fallback when YouTube transcripts are not available.
"""

import os
import yt_dlp
import json
import logging
from datetime import datetime
import time
import tempfile
import shutil

from .audio_transcriber import AudioTranscriber

logger = logging.getLogger(__name__)

class VideoDownloader:
    """
    Class for downloading videos and transcripts from the NH House of Representatives YouTube channel.
    With Whisper speech-to-text fallback when YouTube transcripts are not available.
    """
    
    def __init__(self, channel_url="https://www.youtube.com/@NHHouseofRepresentatives", output_dir="downloads",
                 use_whisper_fallback=True, whisper_model_id="openai/whisper-large-v3-turbo"):
        """
        Initialize the VideoDownloader.
        
        Args:
            channel_url (str): URL of the YouTube channel
            output_dir (str): Directory to store downloads
            use_whisper_fallback (bool): Whether to use Whisper as a fallback for transcription
            whisper_model_id (str): Whisper model ID to use
        """
        self.channel_url = channel_url
        self.output_dir = output_dir
        self.transcript_dir = os.path.join(output_dir, "transcripts")
        self.metadata_dir = os.path.join(output_dir, "metadata")
        self.video_dir = os.path.join(output_dir, "videos")
        self.use_whisper_fallback = use_whisper_fallback
        
        # Create necessary directories
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.transcript_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)
        os.makedirs(self.video_dir, exist_ok=True)
        
        # Initialize Whisper transcriber if fallback is enabled
        if self.use_whisper_fallback:
            self.transcriber = AudioTranscriber(model_id=whisper_model_id)
            logger.info(f"Initialized Whisper audio transcriber with model: {whisper_model_id}")
        
    def get_channel_videos(self, limit=None):
        """
        Get metadata for videos in the channel.
        
        Args:
            limit (int, optional): Maximum number of videos to retrieve
                
        Returns:
            list: List of video metadata dictionaries
        """
        logger.info(f"Fetching videos from channel: {self.channel_url}")
        
        ydl_opts = {
            'extract_flat': True,
            'skip_download': True,
            'quiet': True,
            'playlistreverse': True,  # Get newest videos first
        }
        
        if limit:
            ydl_opts['playlistend'] = limit
            
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(self.channel_url+"/videos", download=False)
                info_live = ydl.extract_info(self.channel_url+"/streams", download=False)
                videos = []
                
                if 'entries' not in info:
                    logger.warning(f"No videos found in channel: {self.channel_url}")
                    return []
                
                all_entries = info.get('entries', [])
                if info_live and 'entries' in info_live:
                    all_entries.extend(info_live['entries'])
                    
                for entry in all_entries:
                    # Make sure we have a video, not a channel or playlist
                    if entry.get('_type') == 'url' and 'id' in entry:
                        video_id = entry['id']
                        video_url = f"https://www.youtube.com/watch?v={video_id}"
                        videos.append({
                            'id': video_id,
                            'title': entry.get('title', 'Unknown Title'),
                            'url': video_url,
                            'upload_date': entry.get('upload_date')
                        })
                
                logger.info(f"Found {len(videos)} videos in channel")
                return videos
                
        except Exception as e:
            logger.error(f"Error fetching channel videos: {e}")
            return []


    def get_detailed_metadata(self, video_id):
        """
        Get detailed metadata for a specific video.
        
        Args:
            video_id (str): YouTube video ID
            
        Returns:
            dict: Video metadata or None if an error occurs
        """
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        metadata_file = os.path.join(self.metadata_dir, f"{video_id}.json")
        
        # Check if we already have metadata cached
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Error reading cached metadata for {video_id}: {e}")
        
        logger.info(f"Fetching detailed metadata for video: {video_id}")
        
        ydl_opts = {
            'skip_download': True,
            'writeinfojson': True,
            'quiet': True,
            'outtmpl': os.path.join(self.metadata_dir, '%(id)s.info'),
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_url, download=False)
                
                # Save metadata to file
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(info, f, ensure_ascii=False, indent=2)
                
                return {
                    'id': info.get('id'),
                    'title': info.get('title'),
                    'description': info.get('description'),
                    'upload_date': info.get('upload_date'),
                    'duration': info.get('duration'),
                    'view_count': info.get('view_count'),
                    'url': video_url,
                    'has_subtitles': bool(info.get('subtitles') or info.get('automatic_captions'))
                }
                
        except Exception as e:
            logger.error(f"Error fetching detailed metadata for {video_id}: {e}")
            return None
    
    def validate_transcript(self, transcript_path):
        """
        Validate that a transcript file is well-formed using webvtt-py.
        
        Args:
            transcript_path (str): Path to the transcript file
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not os.path.exists(transcript_path):
            return False
            
        try:
            import webvtt
            
            # Check file extension to determine format
            _, ext = os.path.splitext(transcript_path)
            ext = ext.lower().lstrip('.')
            
            if ext == 'vtt':
                webvtt.read(transcript_path)
            elif ext == 'srt':
                webvtt.from_srt(transcript_path)
            elif ext == 'sbv':
                webvtt.from_sbv(transcript_path)
            else:
                logger.warning(f"Unsupported transcript format: {ext}")
                return False
                
            return True
            
        except Exception as e:
            logger.warning(f"Invalid transcript file: {transcript_path} - {e}")
            return False
    
    def convert_transcript_format(self, input_path, output_format='vtt'):
        """
        Convert transcript to a different format using webvtt-py.
        
        Args:
            input_path (str): Path to the input transcript file
            output_format (str): Target format ('vtt', 'srt')
            
        Returns:
            str: Path to the converted file, or None if conversion failed
        """
        if not os.path.exists(input_path):
            logger.error(f"Input file does not exist: {input_path}")
            return None
            
        try:
            import webvtt
            
            base_path, ext = os.path.splitext(input_path)
            output_path = f"{base_path}.{output_format}"
            
            # Determine input format from extension
            input_format = ext[1:].lower()
            
            # Handle different input formats
            if input_format == 'vtt':
                vtt = webvtt.read(input_path)
            elif input_format == 'srt':
                vtt = webvtt.from_srt(input_path)
            elif input_format == 'sbv':
                vtt = webvtt.from_sbv(input_path)
            else:
                logger.error(f"Unsupported input format: {input_format}")
                return None
                
            # Save in the desired format
            if output_format == 'vtt':
                vtt.save(output_path)
            elif output_format == 'srt':
                vtt.save_as_srt(output_path)
            else:
                logger.error(f"Unsupported output format: {output_format}")
                return None
                
            logger.info(f"Converted transcript from {input_format} to {output_format}: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error converting transcript format: {e}")
            return None
    
    def download_video_for_whisper(self, video_id):
        """
        Download video for Whisper transcription.
        
        Args:
            video_id (str): YouTube video ID
            
        Returns:
            str: Path to the downloaded video, or None if download failed
        """
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        video_file = os.path.join(self.video_dir, f"{video_id}.mp4")
        
        # Check if video already exists
        if os.path.exists(video_file):
            logger.info(f"Video already exists: {video_file}")
            return video_file
        
        logger.info(f"Downloading video for transcription: {video_id}")
        
        ydl_opts = {
            'format': 'best[ext=mp4]/best',  # Prefer MP4 format
            'outtmpl': os.path.join(self.video_dir, '%(id)s.%(ext)s'),
            'quiet': False,  # Show progress
            'no_warnings': False
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
                
                # Look for the downloaded file
                potential_files = [
                    os.path.join(self.video_dir, f"{video_id}.mp4"),
                    os.path.join(self.video_dir, f"{video_id}.webm"),
                    os.path.join(self.video_dir, f"{video_id}.mkv")
                ]
                
                for file in potential_files:
                    if os.path.exists(file):
                        # If not MP4, rename to match expected format
                        if file != video_file:
                            os.rename(file, video_file)
                            
                        logger.info(f"Downloaded video: {video_file}")
                        return video_file
                
                logger.warning(f"Video download completed but file not found for {video_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading video for {video_id}: {e}")
            return None
    
    def transcribe_with_whisper(self, video_id, format_type='vtt'):
        """
        Transcribe video using Whisper when YouTube captions are not available.
        
        Args:
            video_id (str): YouTube video ID
            format_type (str): Desired transcript format
            
        Returns:
            str: Path to the transcript file, or None if transcription failed
        """
        if not self.use_whisper_fallback:
            logger.warning("Whisper fallback is disabled")
            return None
            
        logger.info(f"Starting Whisper transcription for video: {video_id}")
        
        # Download the video first
        video_path = self.download_video_for_whisper(video_id)
        
        if not video_path:
            logger.error(f"Could not download video for transcription: {video_id}")
            return None
            
        try:
            # Transcribe the video
            transcription = self.transcriber.transcribe_video(video_path, delete_audio=True)
            
            if not transcription:
                logger.error(f"Whisper transcription failed for video: {video_id}")
                return None
                
            logger.info(f"Successfully transcribed video with Whisper: {video_id}")
            
            # Save the transcription in the desired format
            output_path = os.path.join(self.transcript_dir, f"{video_id}.{format_type}")
            
            result_path = self.transcriber.convert_to_format(transcription, output_path, format_type)
            
            if result_path:
                logger.info(f"Saved Whisper transcript in {format_type} format: {result_path}")
                return result_path
            else:
                logger.error(f"Failed to convert Whisper transcript to {format_type} format")
                return None
                
        except Exception as e:
            logger.error(f"Error during Whisper transcription for {video_id}: {e}")
            return None
        finally:
            # Clean up the video file to save disk space
            if os.path.exists(video_path) and video_id in video_path:
                logger.info(f"Cleaning up downloaded video: {video_path}")
                try:
                    os.remove(video_path)
                except Exception as e:
                    logger.warning(f"Could not remove video file: {e}")
    
    def download_transcript(self, video_id, preferred_format='vtt'):
        """
        Download video transcript, prioritizing auto-generated subtitles.
        Falls back to Whisper transcription if YouTube captions are not available.
        
        Args:
            video_id (str): YouTube video ID
            preferred_format (str): Preferred transcript format ('vtt', 'srt')
            
        Returns:
            str: Path to the downloaded transcript file, or None if not available
        """
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        transcript_file = os.path.join(self.transcript_dir, f"{video_id}.{preferred_format}")
        
        # Check if we already have the transcript in preferred format
        if os.path.exists(transcript_file) and self.validate_transcript(transcript_file):
            logger.info(f"Transcript already exists for {video_id}")
            return transcript_file
        
        logger.info(f"Downloading transcript for video: {video_id}")
        
        # First, try to download VTT format (which we'll convert if needed)
        ydl_opts = {
            'skip_download': True,
            'writesubtitles': False,  # Don't try manual subtitles first
            'writeautomaticsub': True,  # Focus on auto-generated subtitles
            'subtitleslangs': ['en'],
            'subtitlesformat': 'vtt',
            'outtmpl': os.path.join(self.transcript_dir, '%(id)s'),
            'quiet': True,
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
                
                # Check for different possible auto-subtitles filenames
                possible_files = [
                    os.path.join(self.transcript_dir, f"{video_id}.en.auto.vtt"),
                    os.path.join(self.transcript_dir, f"{video_id}.en-US.auto.vtt"),
                    os.path.join(self.transcript_dir, f"{video_id}.en-GB.auto.vtt"),
                    # Also check regular subtitle files just in case
                    os.path.join(self.transcript_dir, f"{video_id}.en.vtt"),
                    os.path.join(self.transcript_dir, f"{video_id}.en-US.vtt"),
                    os.path.join(self.transcript_dir, f"{video_id}.en-GB.vtt"),
                ]
                
                # Find the first valid VTT file
                vtt_file = None
                for file in possible_files:
                    if os.path.exists(file) and self.validate_transcript(file):
                        vtt_file = file
                        break
                
                if not vtt_file:
                    logger.warning(f"No transcript found for {video_id} from YouTube")
                    
                    # Fall back to Whisper transcription if enabled
                    if self.use_whisper_fallback:
                        logger.info(f"Falling back to Whisper transcription for {video_id}")
                        whisper_transcript = self.transcribe_with_whisper(video_id, preferred_format)
                        
                        if whisper_transcript:
                            logger.info(f"Successfully generated transcript using Whisper: {whisper_transcript}")
                            return whisper_transcript
                            
                    return None
                    
                # If VTT is the preferred format, rename and return
                if preferred_format.lower() == 'vtt':
                    os.rename(vtt_file, transcript_file)
                    logger.info(f"Transcript downloaded for {video_id}")
                    return transcript_file
                    
                # Otherwise, convert to the preferred format
                converted_file = self.convert_transcript_format(vtt_file, preferred_format)
                if converted_file:
                    logger.info(f"Transcript converted to {preferred_format} for {video_id}")
                    return converted_file
                else:
                    # Fall back to the VTT file
                    os.rename(vtt_file, transcript_file.replace(f".{preferred_format}", ".vtt"))
                    logger.warning(f"Could not convert to {preferred_format}, using VTT for {video_id}")
                    return transcript_file.replace(f".{preferred_format}", ".vtt")
                
        except Exception as e:
            logger.error(f"Error downloading transcript for {video_id}: {e}")
            
            # Fall back to Whisper transcription if enabled and there was an error with yt-dlp
            if self.use_whisper_fallback:
                logger.info(f"Falling back to Whisper transcription for {video_id} after error")
                whisper_transcript = self.transcribe_with_whisper(video_id, preferred_format)
                
                if whisper_transcript:
                    logger.info(f"Successfully generated transcript using Whisper after error: {whisper_transcript}")
                    return whisper_transcript
                    
            return None
            
    def download_transcript_in_format(self, video_id, format_type='vtt'):
        """
        Download or convert transcript to a specific format.
        Falls back to Whisper transcription if YouTube captions are not available.
        
        Args:
            video_id (str): YouTube video ID
            format_type (str): Desired transcript format ('vtt', 'srt', 'sbv')
            
        Returns:
            str: Path to the transcript file in the requested format, or None if not available
        """
        # First, try to see if we already have the transcript in the requested format
        target_file = os.path.join(self.transcript_dir, f"{video_id}.{format_type}")
        if os.path.exists(target_file) and self.validate_transcript(target_file):
            logger.info(f"Transcript already exists in {format_type} format for {video_id}")
            return target_file
            
        # Check if we have the transcript in any supported format
        for ext in ['vtt', 'srt', 'sbv']:
            existing_file = os.path.join(self.transcript_dir, f"{video_id}.{ext}")
            if os.path.exists(existing_file) and self.validate_transcript(existing_file):
                if ext == format_type:
                    return existing_file
                else:
                    # Convert to the requested format
                    converted_file = self.convert_transcript_format(existing_file, format_type)
                    if converted_file:
                        return converted_file
                        
        # If we don't have any transcript, download it in the requested format
        # This will try YouTube captions first, then fall back to Whisper if enabled
        return self.download_transcript(video_id, preferred_format=format_type)