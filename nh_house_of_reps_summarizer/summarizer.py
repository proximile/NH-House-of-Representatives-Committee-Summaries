"""
Transcript summarization for NH House of Representatives videos.
"""

import os
import re
import time
import logging
import openai
import asyncio
import concurrent.futures
from typing import List, Dict, Optional, Any
from utils import chunk_text

logger = logging.getLogger(__name__)

# Global constant for maximum parallel workers
MAX_PARALLEL_WORKERS = 60

class TranscriptSummarizer:
    """
    Class for summarizing transcripts using the Hyperbolic API.
    Support for parallel processing of multiple transcripts and chunked content.
    """
    
    def __init__(self, api_key=None, api_base="https://api.hyperbolic.xyz/v1/", 
                 model_name="deepseek-ai/DeepSeek-V3", max_tokens=64000, chunk_size=120000,
                 max_parallel_workers=MAX_PARALLEL_WORKERS):
        """
        Initialize the transcript summarizer.
        
        Args:
            api_key (str, optional): Hyperbolic API key
            api_base (str, optional): Base URL for the Hyperbolic API
            model_name (str, optional): Model to use for summarization
            max_tokens (int, optional): Maximum tokens for model output
            chunk_size (int, optional): Max characters per chunk for long transcripts
            max_parallel_workers (int, optional): Maximum number of parallel processing workers
        """
        # Set up API key from environment or file
        self.api_key = api_key
        if self.api_key is None:
            api_key_path = os.getenv("HYPERBOLIC_API_KEY_PATH")
            if api_key_path and os.path.exists(api_key_path):
                with open(api_key_path, "r") as f:
                    self.api_key = f.read().strip()
            else:
                self.api_key = os.getenv("HYPERBOLIC_API_KEY")
                
        if not self.api_key:
            raise ValueError("Hyperbolic API key not provided and not found in environment variables")
            
        self.client = openai.Client(api_key=self.api_key, base_url=api_base)
        self.model_name = model_name
        self.max_tokens = max_tokens
        self.chunk_size = chunk_size
        self.max_parallel_workers = max_parallel_workers
        self.prompt_template = """Summarize this transcript from a NH House of Representatives meeting.

Video Title: {title}
{description_text}
Chunk {chunk_number} of {total_chunks}

Transcript:
{raw_vtt_file_contents}

Summarize the above transcript from a NH House of Representatives meeting."""
        
    def _clean_transcript(self, transcript_path):
        """
        Clean and extract plain text from transcript files of various formats.
        
        Args:
            transcript_path (str): Path to the transcript file
            
        Returns:
            str: Plain text extracted from the transcript
        """
        import os
        import re
        
        if not os.path.exists(transcript_path):
            logger.error(f"Transcript file not found: {transcript_path}")
            return ""
        
        file_ext = os.path.splitext(transcript_path)[1].lower()
        
        # First try using the webvtt-py library which handles multiple formats
        try:
            import webvtt
            
            # Try to detect and fix common format issues before parsing
            with open(transcript_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Fix timestamps with comma decimal separators (SRT format in VTT files)
            if file_ext == '.vtt' and re.search(r'\d\d:\d\d:\d\d,\d\d\d', content):
                logger.info(f"Fixing comma-separated timestamps in VTT file: {transcript_path}")
                content = re.sub(r'(\d\d:\d\d:\d\d),(\d\d\d)', r'\1.\2', content)
                
                # Create a temporary fixed file
                temp_path = transcript_path + '.fixed'
                with open(temp_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                # Parse the fixed file
                try:
                    if file_ext == '.vtt':
                        captions = webvtt.read(temp_path)
                    elif file_ext == '.srt':
                        captions = webvtt.from_srt(temp_path)
                    elif file_ext == '.sbv':
                        captions = webvtt.from_sbv(temp_path)
                    else:
                        raise ValueError(f"Unsupported transcript format: {file_ext}")
                    
                    # Clean up the temporary file
                    os.remove(temp_path)
                except Exception as e:
                    logger.warning(f"Error parsing fixed file: {e}")
                    # Clean up the temporary file
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                    raise
            else:
                # Normal parsing without fixing
                if file_ext == '.vtt':
                    captions = webvtt.read(transcript_path)
                elif file_ext == '.srt':
                    captions = webvtt.from_srt(transcript_path)
                elif file_ext == '.sbv':
                    captions = webvtt.from_sbv(transcript_path)
                else:
                    raise ValueError(f"Unsupported transcript format: {file_ext}")
            
            # Extract text from captions
            text_parts = []
            for caption in captions:
                text_parts.append(caption.text)
            
            clean_text = '\n'.join(text_parts)
            
            # Remove HTML tags if present
            clean_text = re.sub(r'<[^>]+>', ' ', clean_text)
            
            # Remove redundant whitespace
            clean_text = re.sub(r'\s+', ' ', clean_text).strip()
            
            logger.info(f"Extracted {len(clean_text)} characters using webvtt-py")
            return clean_text
            
        except Exception as e:
            logger.error(f"Error cleaning transcript with webvtt-py: {str(e)}")
            
            # Fall back to simple regex-based extraction
            try:
                logger.info(f"Falling back to regex-based extraction for {transcript_path}")
                with open(transcript_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Remove header (WEBVTT, styling, etc.)
                if file_ext == '.vtt':
                    content = re.sub(r'^WEBVTT.*?\n\n', '', content, flags=re.DOTALL)
                
                # Remove timestamps and indices
                if file_ext in ['.vtt', '.srt']:
                    # This pattern catches both HH:MM:SS.mmm and HH:MM:SS,mmm formats
                    content = re.sub(r'\d+\n\d\d:\d\d:\d\d[,\.]\d\d\d --> \d\d:\d\d:\d\d[,\.]\d\d\d\n', '\n', content)
                    content = re.sub(r'\d\d:\d\d:\d\d[,\.]\d\d\d --> \d\d:\d\d:\d\d[,\.]\d\d\d\n', '\n', content)
                
                # Clean up the text
                clean_text = re.sub(r'<[^>]+>', ' ', content)  # Remove HTML tags
                clean_text = re.sub(r'\s+', ' ', clean_text).strip()  # Normalize whitespace
                
                logger.info(f"Extracted {len(clean_text)} characters using regex fallback")
                return clean_text
                
            except Exception as fallback_error:
                logger.error(f"Fallback extraction also failed: {str(fallback_error)}")
                return ""
            
    def _clean_transcript_fallback(self, vtt_path):
        """
        Fallback method to process a VTT file using regex patterns.
        
        Args:
            vtt_path (str): Path to the VTT file
            
        Returns:
            str: Cleaned transcript text
        """
        try:
            with open(vtt_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Remove VTT header
            if content.startswith('WEBVTT'):
                content = re.sub(r'^WEBVTT.*?\n\n', '', content, flags=re.DOTALL)
            
            # Remove timestamps and identifiers
            lines = []
            for line in content.split('\n'):
                # Skip empty lines, timestamps, and numeric identifiers
                if not line or re.match(r'^\d+$', line) or re.match(r'^\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}', line):
                    continue
                    
                # Remove speaker identifiers like "SPEAKER:"
                line = re.sub(r'^\s*[A-Z][A-Z\s]+:\s*', '', line)
                
                lines.append(line)
            
            # Join lines into paragraphs
            text = ' '.join(lines)
            
            # Clean up common transcript artifacts
            text = re.sub(r'\[.*?\]', '', text)  # Remove things in brackets
            text = re.sub(r'<.*?>', '', text)    # Remove HTML tags
            text = re.sub(r'\s+', ' ', text)     # Remove excess whitespace
            
            return text.strip()
            
        except Exception as e:
            logger.error(f"Error in fallback transcript cleaning: {e}")
            return ""
            
    def supports_format(self, file_path):
        """
        Check if a transcript file format is supported.
        
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
    
    def _summarize_chunk(self, text_chunk, retry_count=0, max_retries=3, title="", description="", chunk_info=None):
        """
        Summarize a single chunk of text.
        
        Args:
            text_chunk (str): Text chunk to summarize
            retry_count (int): Current retry attempt
            max_retries (int): Maximum number of retries
            title (str): Title of the video
            description (str): Description of the video
            chunk_info (dict): Information about which chunk this is (e.g., {'number': 1, 'total': 5})
            
        Returns:
            str: Summary text or None if failed
        """
        try:
            # Prepare the description text
            description_text = f"Description: {description}" if description else ""
            
            # Prepare chunk information
            chunk_number = chunk_info.get('number', 1) if chunk_info else 1
            total_chunks = chunk_info.get('total', 1) if chunk_info else 1
            
            # Format the prompt with all available information
            prompt = self.prompt_template.format(
                title=title, 
                description_text=description_text, 
                chunk_number=chunk_number,
                total_chunks=total_chunks,
                raw_vtt_file_contents=text_chunk
            )
            
            response = self.client.chat.completions.create(
                model=self.model_name, 
                messages=[{"role": "user", "content": prompt}], 
                max_tokens=self.max_tokens, 
                temperature=0.7, 
                top_p=0.95
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error in API call: {e}")
            if retry_count < max_retries:
                wait_time = (2 ** retry_count) * 3  # Exponential backoff
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                return self._summarize_chunk(text_chunk, retry_count + 1, max_retries, title, description, chunk_info)
            return None
    
    def _process_chunks_parallel(self, chunks, max_retries=3, title="", description=""):
        """
        Process multiple chunks in parallel using ThreadPoolExecutor.
        
        Args:
            chunks (List[str]): List of text chunks to summarize
            max_retries (int): Maximum number of retries for API calls
            title (str): Title of the video
            description (str): Description of the video
            
        Returns:
            List[str]: List of summaries
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_parallel_workers) as executor:
            # Submit all chunks to the executor
            future_to_chunk = {}
            for i, chunk in enumerate(chunks):
                chunk_info = {'number': i+1, 'total': len(chunks)}
                future = executor.submit(self._summarize_chunk, chunk, 0, max_retries, title, description, chunk_info)
                future_to_chunk[future] = i 
            
            # Collect results as they complete
            chunk_summaries = [None] * len(chunks)
            for future in concurrent.futures.as_completed(future_to_chunk):
                idx = future_to_chunk[future]
                try:
                    result = future.result()
                    chunk_summaries[idx] = result
                    logger.info(f"Completed summarization of chunk {idx+1}/{len(chunks)}")
                except Exception as e:
                    logger.error(f"Error processing chunk {idx}: {e}")
            
            # Filter out None results
            return [summary for summary in chunk_summaries if summary]
    
    def summarize(self, transcript_path, max_retries=3, title="", description=""):
        """
        Summarize a transcript.
        
        Args:
            transcript_path (str): Path to the transcript file (VTT, SRT, or SBV)
            max_retries (int, optional): Maximum number of retries for API calls
            title (str): Title of the video
            description (str): Description of the video
            
        Returns:
            str: Summary of the transcript, or None if an error occurred
        """
        logger.info(f"Summarizing transcript: {transcript_path}")
        
        if not self.supports_format(transcript_path):
            logger.warning(f"Unsupported transcript format: {transcript_path}")
            return None
        
        # Clean and preprocess the transcript
        transcript_text = self._clean_transcript(transcript_path)
        if not transcript_text:
            logger.warning(f"No text extracted from transcript: {transcript_path}")
            return None
            
        # For short transcripts, summarize in one go
        if len(transcript_text) <= self.chunk_size:
            return self._summarize_chunk(
                transcript_text, 
                max_retries=max_retries, 
                title=title, 
                description=description, 
                chunk_info={'number': 1, 'total': 1}
            )
        
        # For longer transcripts, split into chunks and summarize each
        logger.info(f"Transcript is long ({len(transcript_text)} chars), splitting into chunks")
        chunks = chunk_text(transcript_text, self.chunk_size)
        
        # Summarize chunks in parallel
        logger.info(f"Summarizing {len(chunks)} chunks in parallel (max workers: {self.max_parallel_workers})")
        chunk_summaries = self._process_chunks_parallel(chunks, max_retries=max_retries, title=title, description=description)
        
        if not chunk_summaries:
            logger.warning("Failed to generate any summaries")
            return None
            
        # If we have multiple chunk summaries, consolidate them
        if len(chunk_summaries) == 1:
            return chunk_summaries[0]
        
        # Create a meta-summary of all chunk summaries
        logger.info("Creating meta-summary from all chunks")
        combined_summary = "\n\n".join(chunk_summaries)
        meta_prompt = f"""The following are summaries of different segments from a NH House of Representatives meeting titled "{title}":

{combined_summary}

Create a cohesive, comprehensive summary that combines all these segments."""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model_name, 
                messages=[{"role": "user", "content": meta_prompt}], 
                max_tokens=self.max_tokens, 
                temperature=0.7, 
                top_p=0.95
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error creating meta-summary: {e}")
            # Fall back to joining the individual summaries
            return "## Combined Summary of Meeting Segments\n\n" + "\n\n".join(chunk_summaries)
    
    def summarize_batch(self, transcript_paths, max_retries=3, video_metadata=None):
        """
        Summarize multiple transcripts in parallel.
        
        Args:
            transcript_paths (List[str]): List of paths to transcript files
            max_retries (int, optional): Maximum number of retries for API calls
            video_metadata (Dict[str, Dict], optional): Dictionary mapping video IDs to their metadata
            
        Returns:
            Dict[str, str]: Dictionary mapping transcript paths to their summaries
        """
        logger.info(f"Batch summarizing {len(transcript_paths)} transcripts")
        
        if video_metadata is None:
            video_metadata = {}
            
        results = {}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_parallel_workers) as executor:
            # Submit all transcripts to the executor
            future_to_path = {}
            
            for path in transcript_paths:
                if not self.supports_format(path):
                    continue
                    
                # Extract video ID from path if possible
                video_id = os.path.basename(path).split('.')[0]
                metadata = video_metadata.get(video_id, {})
                
                future = executor.submit(
                    self.summarize, 
                    path, 
                    max_retries, 
                    metadata.get('title', ''), 
                    metadata.get('description', '')
                )
                future_to_path[future] = path
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_path):
                path = future_to_path[future]
                try:
                    summary = future.result()
                    results[path] = summary
                    logger.info(f"Completed summarization of {path}")
                except Exception as e:
                    logger.error(f"Error summarizing {path}: {e}")
                    results[path] = None
        
        return results


class AsyncTranscriptSummarizer:
    """
    Asynchronous version of TranscriptSummarizer.
    Uses asyncio for parallelization, which can provide better performance
    for I/O-bound operations like API calls.
    """
    
    def __init__(self, api_key=None, api_base="https://api.hyperbolic.xyz/v1/", 
                 model_name="deepseek-ai/DeepSeek-V3", max_tokens=64000, chunk_size=120000,
                 max_parallel_workers=MAX_PARALLEL_WORKERS):
        """
        Initialize the async transcript summarizer.
        
        Args:
            api_key (str, optional): Hyperbolic API key
            api_base (str, optional): Base URL for the Hyperbolic API
            model_name (str, optional): Model to use for summarization
            max_tokens (int, optional): Maximum tokens for model output
            chunk_size (int, optional): Max characters per chunk for long transcripts
            max_parallel_workers (int, optional): Maximum number of parallel processing workers
        """
        # Set up API key from environment or file
        self.api_key = api_key
        if self.api_key is None:
            api_key_path = os.getenv("HYPERBOLIC_API_KEY_PATH")
            if api_key_path and os.path.exists(api_key_path):
                with open(api_key_path, "r") as f:
                    self.api_key = f.read().strip()
            else:
                self.api_key = os.getenv("HYPERBOLIC_API_KEY")
                
        if not self.api_key:
            raise ValueError("Hyperbolic API key not provided and not found in environment variables")
            
        self.client = openai.Client(api_key=self.api_key, base_url=api_base)
        self.model_name = model_name
        self.max_tokens = max_tokens
        self.chunk_size = chunk_size
        self.max_parallel_workers = max_parallel_workers
        self.prompt_template = """Summarize this transcript from a NH House of Representatives meeting.

Video Title: {title}
{description_text}
Chunk {chunk_number} of {total_chunks}

Transcript:
{raw_vtt_file_contents}

Summarize the above transcript from a NH House of Representatives meeting."""
        
    async def _clean_transcript(self, vtt_path):
        """
        Process a VTT file and extract clean text using webvtt-py.
        
        Args:
            vtt_path (str): Path to the VTT file
            
        Returns:
            str: Cleaned transcript text
        """
        if not os.path.exists(vtt_path):
            logger.error(f"Transcript file not found: {vtt_path}")
            return ""
        
        try:
            import webvtt
            
            # Read captions from the VTT file (file I/O operation done in executor)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                captions = await asyncio.get_event_loop().run_in_executor(
                    executor, webvtt.read, vtt_path)
            
            # Extract text from each caption
            transcript_lines = []
            for caption in captions:
                transcript_lines.append(caption.text)
                
            # Join all lines into a single text
            transcript_text = ' '.join(transcript_lines)
            
            # Clean up common transcript artifacts
            transcript_text = re.sub(r'\[.*?\]', '', transcript_text)  # Remove things in brackets
            transcript_text = re.sub(r'\s+', ' ', transcript_text)     # Remove excess whitespace
            
            # Remove speaker identifiers like "SPEAKER:"
            transcript_text = re.sub(r'\b[A-Z][A-Z\s]+:\s*', '', transcript_text)
            
            return transcript_text.strip()
            
        except Exception as e:
            logger.error(f"Error cleaning transcript with webvtt-py: {e}")
            # Fall back to original method if webvtt-py fails
            return await self._clean_transcript_fallback(vtt_path)
            
    async def _clean_transcript_fallback(self, vtt_path):
        """
        Fallback method to process a VTT file using regex patterns.
        
        Args:
            vtt_path (str): Path to the VTT file
            
        Returns:
            str: Cleaned transcript text
        """
        try:
            # Read file asynchronously using executor
            with concurrent.futures.ThreadPoolExecutor() as executor:
                content = await asyncio.get_event_loop().run_in_executor(
                    executor, 
                    lambda: open(vtt_path, 'r', encoding='utf-8').read()
                )
                
            # Remove VTT header
            if content.startswith('WEBVTT'):
                content = re.sub(r'^WEBVTT.*?\n\n', '', content, flags=re.DOTALL)
            
            # Remove timestamps and identifiers
            lines = []
            for line in content.split('\n'):
                # Skip empty lines, timestamps, and numeric identifiers
                if not line or re.match(r'^\d+$', line) or re.match(r'^\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}', line):
                    continue
                    
                # Remove speaker identifiers like "SPEAKER:"
                line = re.sub(r'^\s*[A-Z][A-Z\s]+:\s*', '', line)
                
                lines.append(line)
            
            # Join lines into paragraphs
            text = ' '.join(lines)
            
            # Clean up common transcript artifacts
            text = re.sub(r'\[.*?\]', '', text)  # Remove things in brackets
            text = re.sub(r'<.*?>', '', text)    # Remove HTML tags
            text = re.sub(r'\s+', ' ', text)     # Remove excess whitespace
            
            return text.strip()
            
        except Exception as e:
            logger.error(f"Error in fallback transcript cleaning: {e}")
            return ""
            
    def supports_format(self, file_path):
        """
        Check if a transcript file format is supported.
        
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
    
    async def _summarize_chunk(self, text_chunk, retry_count=0, max_retries=3, title="", description="", chunk_info=None):
        """
        Asynchronously summarize a single chunk of text.
        
        Args:
            text_chunk (str): Text chunk to summarize
            retry_count (int): Current retry attempt
            max_retries (int): Maximum number of retries
            title (str): Title of the video
            description (str): Description of the video
            chunk_info (dict): Information about which chunk this is (e.g., {'number': 1, 'total': 5})
            
        Returns:
            str: Summary text or None if failed
        """
        try:
            # Prepare the description text
            description_text = f"Description: {description}" if description else ""
            
            # Prepare chunk information
            chunk_number = chunk_info.get('number', 1) if chunk_info else 1
            total_chunks = chunk_info.get('total', 1) if chunk_info else 1
            
            # Format the prompt with all available information
            prompt = self.prompt_template.format(
                title=title, 
                description_text=description_text, 
                chunk_number=chunk_number,
                total_chunks=total_chunks,
                raw_vtt_file_contents=text_chunk
            )
            
            # Convert synchronous API call to asynchronous using executor
            with concurrent.futures.ThreadPoolExecutor() as executor:
                response = await asyncio.get_event_loop().run_in_executor(
                    executor,
                    lambda: self.client.chat.completions.create(
                        model=self.model_name, 
                        messages=[{"role": "user", "content": prompt}], 
                        max_tokens=self.max_tokens, 
                        temperature=0.7, 
                        top_p=0.95
                    )
                )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error in API call: {e}")
            if retry_count < max_retries:
                wait_time = (2 ** retry_count) * 3  # Exponential backoff
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
                return await self._summarize_chunk(text_chunk, retry_count + 1, max_retries, title, description, chunk_info)
            return None
    
    async def _process_chunks_parallel(self, chunks, max_retries=3, title="", description=""):
        """
        Process multiple chunks in parallel using asyncio.
        
        Args:
            chunks (List[str]): List of text chunks to summarize
            max_retries (int): Maximum number of retries for API calls
            title (str): Title of the video
            description (str): Description of the video
            
        Returns:
            List[str]: List of summaries
        """
        # Create a semaphore to limit concurrent tasks
        semaphore = asyncio.Semaphore(self.max_parallel_workers)
        
        async def process_with_semaphore(chunk, idx):
            async with semaphore:
                chunk_info = {'number': idx+1, 'total': len(chunks)}
                logger.info(f"Starting summarization of chunk {idx+1}/{len(chunks)}")
                result = await self._summarize_chunk(
                    chunk, 
                    max_retries=max_retries,
                    title=title,
                    description=description,
                    chunk_info=chunk_info
                )
                logger.info(f"Completed summarization of chunk {idx+1}/{len(chunks)}")
                return result
        
        # Create and gather all tasks
        tasks = [process_with_semaphore(chunk, i) for i, chunk in enumerate(chunks)]
        chunk_summaries = await asyncio.gather(*tasks)
        
        # Filter out None results
        return [summary for summary in chunk_summaries if summary]
    
    async def summarize(self, transcript_path, max_retries=3, title="", description=""):
        """
        Summarize a transcript asynchronously.
        
        Args:
            transcript_path (str): Path to the transcript file
            max_retries (int, optional): Maximum number of retries for API calls
            title (str): Title of the video
            description (str): Description of the video
            
        Returns:
            str: Summary of the transcript, or None if an error occurred
        """
        logger.info(f"Asynchronously summarizing transcript: {transcript_path}")
        
        if not self.supports_format(transcript_path):
            logger.warning(f"Unsupported transcript format: {transcript_path}")
            return None
        
        # Clean and preprocess the transcript
        transcript_text = await self._clean_transcript(transcript_path)
        if not transcript_text:
            logger.warning(f"No text extracted from transcript: {transcript_path}")
            return None
            
        # For short transcripts, summarize in one go
        if len(transcript_text) <= self.chunk_size:
            return await self._summarize_chunk(
                transcript_text, 
                max_retries=max_retries,
                title=title,
                description=description,
                chunk_info={'number': 1, 'total': 1}
            )
        
        # For longer transcripts, split into chunks and summarize each
        logger.info(f"Transcript is long ({len(transcript_text)} chars), splitting into chunks")
        chunks = chunk_text(transcript_text, self.chunk_size)
        
        # Summarize chunks in parallel
        logger.info(f"Summarizing {len(chunks)} chunks in parallel (max workers: {self.max_parallel_workers})")
        chunk_summaries = await self._process_chunks_parallel(
            chunks, 
            max_retries=max_retries,
            title=title,
            description=description
        )
        
        if not chunk_summaries:
            logger.warning("Failed to generate any summaries")
            return None
            
        # If we have multiple chunk summaries, consolidate them
        if len(chunk_summaries) == 1:
            return chunk_summaries[0]
        
        # Create a meta-summary of all chunk summaries
        logger.info("Creating meta-summary from all chunks")
        combined_summary = "\n\n".join(chunk_summaries)
        meta_prompt = f"""The following are summaries of different segments from a NH House of Representatives meeting titled "{title}":

{combined_summary}

Create a cohesive, comprehensive summary that combines all these segments."""
        
        try:
            # Convert synchronous API call to asynchronous using executor
            with concurrent.futures.ThreadPoolExecutor() as executor:
                response = await asyncio.get_event_loop().run_in_executor(
                    executor,
                    lambda: self.client.chat.completions.create(
                        model=self.model_name, 
                        messages=[{"role": "user", "content": meta_prompt}], 
                        max_tokens=self.max_tokens, 
                        temperature=0.7, 
                        top_p=0.95
                    )
                )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error creating meta-summary: {e}")
            # Fall back to joining the individual summaries
            return "## Combined Summary of Meeting Segments\n\n" + "\n\n".join(chunk_summaries)
    
    async def summarize_batch(self, transcript_paths, max_retries=3, video_metadata=None):
        """
        Summarize multiple transcripts in parallel using asyncio.
        
        Args:
            transcript_paths (List[str]): List of paths to transcript files
            max_retries (int, optional): Maximum number of retries for API calls
            video_metadata (Dict[str, Dict], optional): Dictionary mapping video IDs to their metadata
            
        Returns:
            Dict[str, str]: Dictionary mapping transcript paths to their summaries
        """
        logger.info(f"Batch summarizing {len(transcript_paths)} transcripts asynchronously")
        
        if video_metadata is None:
            video_metadata = {}
            
        # Create a semaphore to limit concurrent tasks
        semaphore = asyncio.Semaphore(self.max_parallel_workers)
        
        async def process_with_semaphore(path):
            if not self.supports_format(path):
                logger.warning(f"Unsupported transcript format: {path}")
                return path, None
                
            # Extract video ID from path if possible
            video_id = os.path.basename(path).split('.')[0]
            metadata = video_metadata.get(video_id, {})
                
            async with semaphore:
                logger.info(f"Starting summarization of {path}")
                result = await self.summarize(
                    path, 
                    max_retries=max_retries,
                    title=metadata.get('title', ''),
                    description=metadata.get('description', '')
                )
                logger.info(f"Completed summarization of {path}")
                return path, result
        
        # Create and gather all tasks
        tasks = [process_with_semaphore(path) for path in transcript_paths]
        results = await asyncio.gather(*tasks)
        
        # Convert results to dictionary
        return {path: summary for path, summary in results}


# Helper function to run the batch summarization using the async version
def summarize_batch_async(transcript_paths, video_metadata=None, **kwargs):
    """
    Helper function to run async batch summarization from synchronous code.
    
    Args:
        transcript_paths (List[str]): List of paths to transcript files
        video_metadata (Dict[str, Dict], optional): Dictionary mapping video IDs to their metadata
        **kwargs: Additional arguments to pass to AsyncTranscriptSummarizer
        
    Returns:
        Dict[str, str]: Dictionary mapping transcript paths to their summaries
    """
    async def run_batch():
        summarizer = AsyncTranscriptSummarizer(**kwargs)
        return await summarizer.summarize_batch(transcript_paths, video_metadata=video_metadata)
        
    return asyncio.run(run_batch())