"""
Audio transcription pipeline using Whisper for the NH House of Representatives Video Summarizer.
"""

import os
import logging
import time
import tempfile
import subprocess
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class AudioTranscriber:
    """
    Class for transcribing audio from videos using Whisper when YouTube captions are unavailable.
    """
    
    def __init__(self, model_id="openai/whisper-large-v3-turbo", chunk_length_s=30, batch_size=16):
        """
        Initialize the AudioTranscriber.
        
        Args:
            model_id (str): Identifier of the Whisper model on Hugging Face Hub
            chunk_length_s (int): Length of each audio chunk in seconds
            batch_size (int): Number of chunks to process simultaneously
        """
        self.model_id = model_id
        self.chunk_length_s = chunk_length_s
        self.batch_size = batch_size
        self._check_dependencies()
        
    def _check_dependencies(self):
        """Check if required dependencies are installed."""
        try:
            import torch
            import transformers
            import librosa
            self.dependencies_installed = True
            
            # Log GPU availability
            if torch.cuda.is_available():
                logger.info(f"GPU available for transcription: {torch.cuda.get_device_name(0)}")
                logger.info(f"Using torch dtype: float16")
            else:
                logger.info("No GPU available for transcription. Using CPU (this will be slow)")
                logger.info(f"Using torch dtype: float32")
                
        except ImportError as e:
            logger.warning(f"Missing dependencies for Whisper transcription: {e}")
            logger.warning("Install with: pip install torch transformers librosa")
            self.dependencies_installed = False
    
    def transcribe_long_audio(self, file_path):
        if not self.dependencies_installed:
            logger.error("Required dependencies are not installed. Cannot transcribe audio.")
            return None
            
        if not os.path.exists(file_path):
            logger.error(f"Audio file not found: {file_path}")
            return None
            
        logger.info(f"Transcribing audio file: {file_path} (size: {os.path.getsize(file_path)} bytes)")
        start_time = time.time()
        
        try:
            import torch
            from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor, pipeline
            import librosa
            
            # Set device and data type based on GPU availability
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32
            
            logger.info(f"Loading Whisper model: {self.model_id} (device: {device})")
            
            # Load the model and processor
            model = AutoModelForSpeechSeq2Seq.from_pretrained(
                self.model_id, 
                torch_dtype=torch_dtype, 
                low_cpu_mem_usage=True
            ).to(device)
            
            processor = AutoProcessor.from_pretrained(self.model_id)
            logger.info("Model and processor loaded successfully")
            
            # Initialize the pipeline
            asr_pipeline = pipeline(
                "automatic-speech-recognition",
                model=model,
                tokenizer=processor.tokenizer,
                feature_extractor=processor.feature_extractor,
                chunk_length_s=self.chunk_length_s,
                batch_size=self.batch_size,
                torch_dtype=torch_dtype,
                device=device,
            )
            logger.info("ASR pipeline initialized")
            
            logger.info(f"Loading audio file: {file_path}")
            
            # Load and preprocess the audio
            try:
                audio, sampling_rate = librosa.load(file_path, sr=processor.feature_extractor.sampling_rate)
                logger.info(f"Audio loaded successfully, length: {len(audio)/sampling_rate:.2f} seconds")
            except Exception as e:
                logger.error(f"Error loading audio: {e}")
                return None
                
            inputs = {"array": audio, "sampling_rate": sampling_rate}
            
            logger.info(f"Starting transcription with Whisper (chunk_length_s={self.chunk_length_s}, batch_size={self.batch_size})...")
            
            # Perform transcription with progress updates
            result = asr_pipeline(inputs, return_timestamps=True)
            transcription = result["text"]
            
            elapsed_time = time.time() - start_time
            logger.info(f"Transcription completed in {elapsed_time:.2f} seconds")
            logger.info(f"Transcription result: {len(transcription)} characters")
            
            return transcription
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"Error during audio transcription after {elapsed_time:.2f} seconds: {e}")
            return None
    
    def extract_audio_from_video(self, video_path, output_path=None):
        if not os.path.exists(video_path):
            logger.error(f"Video file not found: {video_path}")
            return None
            
        if output_path is None:
            fd, output_path = tempfile.mkstemp(suffix=".mp3")
            os.close(fd)
            
        logger.info(f"Extracting audio from video: {video_path}")
        
        try:
            # Add verbosity control to ffmpeg
            cmd = [
                "ffmpeg",
                "-i", video_path,
                "-q:a", "0",
                "-map", "a",
                "-y",  # Overwrite output file if it exists
                "-loglevel", "error",  # Reduce verbosity
                output_path
            ]
            
            # Run with timeout to prevent hanging
            process = subprocess.run(
                cmd,
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
                timeout=300  # 5-minute timeout
            )
            
            if process.returncode != 0:
                logger.error(f"Error extracting audio: {process.stderr}")
                return None
                
            # Verify the file was created
            if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
                logger.error(f"Audio extraction produced empty file: {output_path}")
                return None
                
            logger.info(f"Audio extracted successfully: {output_path} ({os.path.getsize(output_path)} bytes)")
            return output_path
            
        except subprocess.TimeoutExpired:
            logger.error(f"Audio extraction timed out after 5 minutes")
            return None
        except Exception as e:
            logger.error(f"Error during audio extraction: {e}")
            return None
            
    def transcribe_video(self, video_path, delete_audio=True):
        """
        Extract audio from video and transcribe it.
        
        Args:
            video_path (str): Path to the video file
            delete_audio (bool): Whether to delete the extracted audio file after transcription
            
        Returns:
            str: The transcribed text, or None if transcription failed
        """
        audio_path = self.extract_audio_from_video(video_path)
        if not audio_path:
            return None
            
        try:
            transcription = self.transcribe_long_audio(audio_path)
            
            if delete_audio and os.path.exists(audio_path):
                os.remove(audio_path)
                logger.info(f"Deleted temporary audio file: {audio_path}")
                
            return transcription
            
        except Exception as e:
            logger.error(f"Error during video transcription: {e}")
            
            if delete_audio and os.path.exists(audio_path):
                os.remove(audio_path)
                
            return None
            
    def create_vtt_from_text(self, text, output_path):
        """
        Create a WebVTT file from plain text by segmenting it.
        
        Args:
            text (str): Plain text transcription
            output_path (str): Path to save the VTT file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Simple sentence splitting 
            import re
            sentences = re.split(r'(?<=[.!?])\s+', text)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("WEBVTT\n\n")
                
                # Estimate ~10 words per 5 seconds
                start_time = 0
                for i, sentence in enumerate(sentences):
                    # Estimate duration based on word count (rough approximation)
                    word_count = len(sentence.split())
                    duration = max(2, word_count * 0.5)  # at least 2 seconds per segment
                    
                    # Format timestamps
                    start = self._format_timestamp(start_time)
                    end = self._format_timestamp(start_time + duration)
                    
                    # Write cue
                    f.write(f"{i+1}\n")
                    f.write(f"{start} --> {end}\n")
                    f.write(f"{sentence.strip()}\n\n")
                    
                    # Update start time for next segment
                    start_time += duration
                    
            logger.info(f"Created VTT file from transcription: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating VTT file: {e}")
            return False
            
    def _format_timestamp(self, seconds):
        """
        Format seconds as WebVTT timestamp (HH:MM:SS.mmm).
        
        Args:
            seconds (float): Time in seconds
            
        Returns:
            str: Formatted timestamp
        """
        # Handle negative time (shouldn't happen but just in case)
        seconds = max(0, seconds)
        
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = seconds % 60
        
        return f"{hours:02d}:{minutes:02d}:{seconds:06.3f}".replace(".", ",")
            
    def convert_to_format(self, text, output_path, format_type='vtt'):
        """
        Convert plain text transcription to the requested subtitle format.
        
        Args:
            text (str): Transcribed text
            output_path (str): Path to save the output file
            format_type (str): Subtitle format ('vtt', 'srt', or 'sbv')
            
        Returns:
            str: Path to the subtitle file, or None if conversion failed
        """
        if format_type.lower() == 'vtt':
            if self.create_vtt_from_text(text, output_path):
                return output_path
                
        elif format_type.lower() in ['srt', 'sbv']:
            # First create VTT
            vtt_path = output_path.replace(f".{format_type}", ".vtt")
            if self.create_vtt_from_text(text, vtt_path):
                # Then convert to desired format using existing converter
                try:
                    import webvtt
                    vtt = webvtt.read(vtt_path)
                    
                    if format_type.lower() == 'srt':
                        vtt.save_as_srt(output_path)
                    # Add more format conversions as needed
                    
                    # Clean up temporary VTT file
                    os.remove(vtt_path)
                    
                    return output_path
                except Exception as e:
                    logger.error(f"Error converting to {format_type}: {e}")
                    return vtt_path  # Return VTT as fallback
        
        return None
