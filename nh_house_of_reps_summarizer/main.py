"""
Main application logic for the NH House of Representatives Video Summarizer.
With enhanced parallel processing capabilities, immediate summary file export,
and Whisper transcription fallback.
"""

import os
import time
import logging
from datetime import datetime
# Import the entire utils module instead of individual functions
import utils
from video_downloader import VideoDownloader
from database import DatabaseManager
from summarizer import TranscriptSummarizer, AsyncTranscriptSummarizer, summarize_batch_async, MAX_PARALLEL_WORKERS

logger = logging.getLogger(__name__)

class NHVideoProcessor:
    """
    Main class for processing NH House of Representatives videos.
    Enhanced with parallel processing capabilities, immediate summary file export,
    and Whisper transcription fallback.
    """
    
    def __init__(self, api_key=None, download_dir="downloads", db_path="nh_videos.db", 
                 output_dir="summaries", max_parallel_workers=None, use_whisper_fallback=True,
                 whisper_model_id="openai/whisper-large-v3-turbo", whisper_chunk_length=30,
                 whisper_batch_size=16, force_whisper=False):
        """
        Initialize the NH Video Processor.
        
        Args:
            api_key (str, optional): Hyperbolic API key
            download_dir (str, optional): Directory for downloads
            db_path (str, optional): Path to the database file
            output_dir (str, optional): Directory for saving summary files
            max_parallel_workers (int, optional): Maximum number of parallel workers for summarization
            use_whisper_fallback (bool, optional): Whether to use Whisper for transcription when YouTube captions aren't available
            whisper_model_id (str, optional): Whisper model ID to use
            whisper_chunk_length (int, optional): Chunk length in seconds for Whisper processing
            whisper_batch_size (int, optional): Batch size for Whisper processing
            force_whisper (bool, optional): Whether to force using Whisper even if YouTube captions are available
        """
        # Initialize the downloader with Whisper support
        self.downloader = VideoDownloader(
            output_dir=download_dir,
            use_whisper_fallback=use_whisper_fallback,
            whisper_model_id=whisper_model_id
        )
        
        # Set additional Whisper parameters
        if hasattr(self.downloader, 'transcriber'):
            self.downloader.transcriber.chunk_length_s = whisper_chunk_length
            self.downloader.transcriber.batch_size = whisper_batch_size
            
        # Set force_whisper flag if specified
        if force_whisper:
            self.downloader.force_whisper = True
            
        self.db = DatabaseManager(db_path=db_path)
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Set the number of parallel workers (default to global constant if not specified)
        self.max_parallel_workers = max_parallel_workers or MAX_PARALLEL_WORKERS
        
        # Initialize the summarizer with the specified number of workers
        self.summarizer = TranscriptSummarizer(
            api_key=api_key,
            max_parallel_workers=self.max_parallel_workers
        )
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
    def fetch_channel_videos(self, limit=None):
        """
        Fetch videos from the channel and store in database.
        
        Args:
            limit (int, optional): Maximum number of videos to fetch
            
        Returns:
            int: Number of videos fetched
        """
        logger.info(f"Fetching videos from channel (limit={limit})")
        
        videos = self.downloader.get_channel_videos(limit=limit)
        count = 0
        
        for video in videos:
            # Get detailed metadata
            metadata = self.downloader.get_detailed_metadata(video['id'])
            if not metadata:
                logger.warning(f"Could not get metadata for video: {video['id']}")
                continue
                
            # Add to database (without transcript path for now)
            if self.db.add_video(metadata):
                count += 1
                
        logger.info(f"Finished fetching videos: added/updated {count} videos")
        return count
    
    def download_transcripts(self, limit=None, format_type='vtt', batch_size=10):
        """
        Download transcripts for videos in the database.
        
        Args:
            limit (int, optional): Maximum number of videos to process
            format_type (str, optional): Transcript format to download ('vtt', 'srt', 'sbv')
            batch_size (int, optional): Number of transcripts to download in each batch
            
        Returns:
            int: Number of transcripts downloaded
        """
        logger.info(f"Downloading transcripts for videos (limit={limit}, format={format_type})")
        
        if not utils.is_transcript_format_supported(f"dummy.{format_type}"):
            logger.error(f"Unsupported transcript format: {format_type}")
            return 0
        
        count = 0
        
        try:
            # First, check if we have any videos in the database
            import sqlite3
            conn = sqlite3.connect(self.db.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM videos")
            video_count = cursor.fetchone()[0]
            
            if video_count == 0:
                logger.warning("No videos in database. Run 'nh_house_of_reps_summarizer fetch' first.")
                conn.close()
                return 0
            
            # Get videos without transcripts
            query = '''
            SELECT id FROM videos
            WHERE transcript_path IS NULL 
            ORDER BY upload_date DESC
            '''
            
            if limit:
                query += f" LIMIT {limit}"
                
            cursor.execute(query)
            videos = cursor.fetchall()
            conn.close()
            
            if not videos:
                logger.info("No videos without transcripts found in database")
                return 0
            
            # Process videos in batches
            video_ids = [vid[0] for vid in videos]
            total_videos = len(video_ids)
            
            for i in range(0, total_videos, batch_size):
                batch = video_ids[i:i+batch_size]
                logger.info(f"Processing transcript download batch {i//batch_size + 1}/{(total_videos-1)//batch_size + 1} ({len(batch)} videos)")
                
                for video_id in batch:
                    logger.info(f"Downloading transcript for video: {video_id}")
                    transcript_path = self.downloader.download_transcript_in_format(video_id, format_type=format_type)
                    
                    if transcript_path and os.path.exists(transcript_path):
                        # Update database with transcript path
                        if self.db.update_transcript_path(video_id, transcript_path):
                            count += 1
                            logger.info(f"Downloaded transcript for video: {video_id} in {format_type} format")
                        else:
                            logger.warning(f"Failed to update database for video: {video_id}")
                    else:
                        logger.warning(f"Failed to download transcript for video: {video_id}")
                    
                    # Be nice to the API
                    time.sleep(1)
                
            logger.info(f"Finished downloading transcripts: {count} transcripts downloaded")
            return count
            
        except Exception as e:
            logger.error(f"Error downloading transcripts: {e}")
            return count

    def process_videos(self, limit=5, delay=2, organize_by_date=True, filename_template="{id}_{title}.md", 
                       overwrite=False, async_mode=False):
        """
        Process unprocessed videos, generate summaries, and immediately save to text files.
        
        Args:
            limit (int, optional): Maximum number of videos to process
            delay (int, optional): Delay between API calls in seconds
            organize_by_date (bool, optional): Whether to organize by date in YYYY/MM subdirectories
            filename_template (str, optional): Template for filenames
            overwrite (bool, optional): Whether to overwrite existing summary files
            async_mode (bool, optional): Whether to use async processing (uses asyncio)
            
        Returns:
            int: Number of videos processed
        """
        import concurrent.futures
        import time
        import os
        
        logger.info(f"Processing videos for summarization (limit={limit}, parallel_workers={self.max_parallel_workers})")
        
        # Get videos without summaries
        logger.info("Checking for videos without summaries...")
        query = '''
        SELECT v.* FROM videos v
        LEFT JOIN summaries s ON v.id = s.video_id
        WHERE s.video_id IS NULL AND v.transcript_path IS NOT NULL
        ORDER BY v.upload_date DESC
        '''
        
        if limit:
            query += f" LIMIT {limit}"
        
        import sqlite3
        conn = sqlite3.connect(self.db.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query)
        videos = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        if not videos:
            logger.info("No videos without summaries found in database")
            return 0
            
        logger.info(f"Found {len(videos)} videos without summaries")
        
        # Filter videos to only those with valid transcript paths
        valid_videos = []
        for video in videos:
            if not video['transcript_path'] or not os.path.exists(video['transcript_path']):
                logger.warning(f"Transcript file not found for video: {video['id']}")
                continue
                
            # Skip if summary file exists and overwrite is False
            summary_path = self._get_summary_path(video, organize_by_date, filename_template)
            if os.path.exists(summary_path) and not overwrite:
                logger.info(f"Summary file already exists for video {video['id']}, skipping")
                continue
                
            # Verify the transcript format is supported
            if not utils.is_transcript_format_supported(video['transcript_path']):
                logger.warning(f"Unsupported transcript format for video: {video['id']}")
                continue
                
            valid_videos.append(video)
            
        if not valid_videos:
            logger.warning("No videos with valid transcripts found")
            return 0
        
        # Decide which processing method to use
        if async_mode:
            logger.info("Using async processing mode")
            return self._process_videos_async(valid_videos, delay, organize_by_date, filename_template, overwrite)
        
        # Process one video at a time
        count = 0
        for video in valid_videos:
            try:
                video_id = video['id']
                logger.info(f"Processing video {video_id}")
                
                # Read and clean the transcript
                transcript_path = video['transcript_path']
                logger.info(f"Reading transcript from {transcript_path}")
                
                try:
                    transcript_text = self.summarizer._clean_transcript(transcript_path)
                    logger.info(f"Cleaned transcript length: {len(transcript_text)} characters")
                    
                    if not transcript_text:
                        logger.warning(f"No text extracted from transcript: {transcript_path}")
                        continue
                except Exception as e:
                    logger.error(f"Error cleaning transcript for {video_id}: {str(e)}")
                    continue
                
                # Process this transcript and generate summary
                try:
                    # Determine if we need to chunk the transcript
                    if len(transcript_text) <= self.summarizer.chunk_size:
                        # Single chunk case - process directly
                        logger.info(f"Processing video {video_id} as a single chunk")
                        summary = self.summarizer._summarize_chunk(
                            transcript_text,
                            0,  # retry_count
                            3,  # max_retries
                            title=video.get('title', ''),
                            description=video.get('description', ''),
                            chunk_info={'number': 1, 'total': 1}
                        )
                    else:
                        # Multi-chunk case - using the proper chunking function from utils.py
                        logger.info(f"Transcript for {video_id} is long ({len(transcript_text)} chars), splitting into chunks")
                        
                        try:
                            # Use chunk_text from utils.py with overlap to ensure context is preserved between chunks
                            overlap = 100  # Characters of overlap between chunks to maintain context
                            chunks = utils.chunk_text(transcript_text, self.summarizer.chunk_size, overlap)
                            logger.info(f"Successfully split into {len(chunks)} chunks using natural language boundaries")
                        except Exception as e:
                            logger.error(f"Error during chunking for {video_id}: {str(e)}")
                            continue
                        
                        # Process chunks with limited concurrency
                        chunk_summaries = []
                        logger.info(f"Processing {len(chunks)} chunks with limited concurrency")
                        
                        # Use a thread pool with a reasonable number of workers
                        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                            # Submit all chunks to the executor
                            future_to_chunk = {}
                            for i, chunked_text in enumerate(chunks):
                                logger.info(f"Submitting chunk {i+1}/{len(chunks)} for processing (length: {len(chunked_text)} chars)")
                                future = executor.submit(
                                    self.summarizer._summarize_chunk, 
                                    chunked_text,
                                    0,  # retry_count
                                    3,  # max_retries
                                    video.get('title', ''),
                                    video.get('description', ''),
                                    {'number': i+1, 'total': len(chunks)}
                                )
                                future_to_chunk[future] = i
                                time.sleep(1)  # Add delay between submissions
                            
                            # Process results as they complete
                            for future in concurrent.futures.as_completed(future_to_chunk):
                                chunk_idx = future_to_chunk[future]
                                try:
                                    chunk_summary = future.result()
                                    if chunk_summary:
                                        chunk_summaries.append(chunk_summary)
                                        logger.info(f"Completed chunk {chunk_idx+1}/{len(chunks)}")
                                    else:
                                        logger.warning(f"No summary generated for chunk {chunk_idx+1}")
                                except Exception as e:
                                    logger.error(f"Error processing chunk {chunk_idx+1}: {str(e)}")
                                
                                time.sleep(0.5)  # Small delay after each completion
                        
                        # Create a meta-summary from all chunk summaries
                        if not chunk_summaries:
                            logger.warning(f"No valid chunk summaries generated for video {video_id}")
                            continue
                            
                        if len(chunk_summaries) == 1:
                            summary = chunk_summaries[0]
                        else:
                            # Generate meta-summary
                            logger.info(f"Creating meta-summary from {len(chunk_summaries)} chunks")
                            combined_summary = "\n\n".join(chunk_summaries)
                            meta_prompt = f"""The following are summaries of different segments from a NH House of Representatives meeting titled "{video.get('title', '')}":
{video.get('description', '') and f"\nDescription: {video.get('description', '')}\n" or ""}

{"\n".join([f"Segment {i+1}/{len(chunk_summaries)}: {summary}" for i, summary in enumerate(chunk_summaries)])}

Create a cohesive, comprehensive summary that combines all these segments."""

                            try:
                                response = self.summarizer.client.chat.completions.create(
                                    model=self.summarizer.model_name, 
                                    messages=[{"role": "user", "content": meta_prompt}], 
                                    max_tokens=self.summarizer.max_tokens, 
                                    temperature=0.7, 
                                    top_p=0.95
                                )
                                summary = response.choices[0].message.content
                            except Exception as e:
                                logger.error(f"Error creating meta-summary: {str(e)}")
                                # Fall back to joining the individual summaries
                                summary = "## Combined Summary of Meeting Segments\n\n" + "\n\n".join(chunk_summaries)
                    
                    # Save the summary to the database
                    if summary and self.db.save_summary(video_id, summary):
                        logger.info(f"Generated and saved summary to database for video: {video_id}")
                        
                        # Immediately write the summary to a text file
                        try:
                            summary_path = self._get_summary_path(video, organize_by_date, filename_template)
                            
                            # Create directory if it doesn't exist
                            os.makedirs(os.path.dirname(summary_path), exist_ok=True)
                            
                            with open(summary_path, 'w', encoding='utf-8') as f:
                                f.write(f"Title: {video['title']}\n")
                                f.write(f"URL: {video['url']}\n")
                                f.write(f"Upload Date: {utils.format_date(video['upload_date'])}\n")
                                f.write(f"Video ID: {video['id']}\n\n")
                                f.write(summary)
                                
                            logger.info(f"Saved summary to file: {summary_path}")
                            count += 1
                        except Exception as e:
                            logger.error(f"Error writing summary file for {video_id}: {str(e)}")
                    else:
                        logger.warning(f"Failed to save summary for video: {video_id}")
                        
                except Exception as e:
                    logger.error(f"Error processing video {video_id}: {str(e)}")
                    
                # Add delay between videos
                time.sleep(delay)
                    
            except Exception as e:
                logger.error(f"Unexpected error processing video {video['id']}: {str(e)}")
        
        logger.info(f"Finished processing videos: {count} videos summarized")
        return count
        
    def _get_summary_path(self, video, organize_by_date=True, filename_template="{id}_{title}.md"):
        """
        Determine the file path for a summary based on video metadata.
        
        Args:
            video (dict): Video metadata
            organize_by_date (bool): Whether to organize by date in YYYY/MM subdirectories
            filename_template (str): Template for filenames
            
        Returns:
            str: Path where the summary file should be saved
        """
        # Format the filename
        filename = filename_template
        
        if "{id}" in filename:
            filename = filename.replace("{id}", video.get('id', 'unknown'))
            
        if "{title}" in filename:
            title = utils.clean_filename(video.get('title', 'Unknown Title'))
            # Truncate title if too long (max 100 chars)
            if len(title) > 100:
                title = title[:97] + "..."
            filename = filename.replace("{title}", title)
            
        if "{date}" in filename and video.get('upload_date'):
            date = utils.format_date(video['upload_date'])
            filename = filename.replace("{date}", date)
        
        # Determine directory path
        if organize_by_date and video.get('upload_date'):
            date_str = video['upload_date']
            if len(date_str) == 8:  # Format: YYYYMMDD
                year = date_str[0:4]
                month = date_str[4:6]
                
                # Create year/month directory structure
                date_dir = os.path.join(self.output_dir, year, month)
                return os.path.join(date_dir, filename)
        
        # Default case - no date organization
        return os.path.join(self.output_dir, filename)

    def run_pipeline(self, fetch_limit=50, process_limit=10, delay=2, transcript_format='vtt', 
                     organize_by_date=True, filename_template="{id}_{title}.md", overwrite=False,
                     async_mode=False, max_parallel_workers=None):
        """
        Run the full pipeline: fetch videos, download transcripts, and generate summaries with file output.
        
        Args:
            fetch_limit (int, optional): Maximum number of videos to fetch
            process_limit (int, optional): Maximum number of videos to process
            delay (int, optional): Delay between API calls in seconds
            transcript_format (str, optional): Format to use for transcripts ('vtt', 'srt', 'sbv')
            organize_by_date (bool, optional): Whether to organize summary files by date
            filename_template (str, optional): Template for summary filenames
            overwrite (bool, optional): Whether to overwrite existing summary files
            async_mode (bool, optional): Whether to use asyncio-based processing for summarization
            max_parallel_workers (int, optional): Override the default number of parallel workers
            
        Returns:
            tuple: Number of videos fetched, transcripts downloaded, and processed
        """
        logger.info(f"Starting full pipeline with transcript format: {transcript_format}")
        
        # Fetch videos from channel
        fetch_count = self.fetch_channel_videos(limit=fetch_limit)
        
        # Download transcripts
        transcript_count = self.download_transcripts(
            limit=fetch_limit, 
            format_type=transcript_format
        )
        
        # Process videos and generate summaries (with immediate file output)
        process_count = self.process_videos(
            limit=process_limit,
            delay=delay,
            organize_by_date=organize_by_date,
            filename_template=filename_template,
            overwrite=overwrite,
            async_mode=async_mode
        )
        
        logger.info(f"Pipeline completed: fetched {fetch_count} videos, downloaded {transcript_count} transcripts, processed {process_count} videos")
        return (fetch_count, transcript_count, process_count)
    
    def get_video_summary(self, video_id):
        """
        Get a video and its summary.
        
        Args:
            video_id (str): Video ID
            
        Returns:
            dict: Video data including summary, or None if not found
        """
        return self.db.get_video_with_summary(video_id)
    
    def search_videos(self, query, limit=20):
        """
        Search for videos by title, description, or summary.
        
        Args:
            query (str): Search query
            limit (int, optional): Maximum number of results to return
            
        Returns:
            list: List of video dictionaries
        """
        return self.db.search_videos(query, limit=limit)
        
    def convert_transcript(self, video_id, target_format):
        """
        Convert a video's transcript to a different format.
        
        Args:
            video_id (str): Video ID
            target_format (str): Target format ('vtt', 'srt')
            
        Returns:
            str: Path to the converted transcript, or None if conversion failed
        """
        if not is_transcript_format_supported(f"dummy.{target_format}"):
            logger.error(f"Unsupported target format: {target_format}")
            return None
            
        # Get the video's current transcript path
        video = self.db.get_video_with_summary(video_id)
        if not video or not video.get('transcript_path'):
            logger.error(f"No transcript found for video: {video_id}")
            return None
            
        current_path = video['transcript_path']
        if not os.path.exists(current_path):
            logger.error(f"Transcript file not found: {current_path}")
            return None
            
        # Convert the transcript
        converted_path = self.downloader.convert_transcript_format(current_path, target_format)
        if converted_path:
            # Update the database with the new path
            self.db.update_transcript_path(video_id, converted_path)
            return converted_path
            
        return None
    
    def _process_videos_threaded(self, videos, delay=2, organize_by_date=True, filename_template="{id}_{title}.md", 
                                overwrite=False):
        """
        Process videos using thread-based parallelism with a flat structure.

        Args:
            videos (list): List of video dictionaries with valid transcripts
            delay (int): Delay between API calls in seconds
            organize_by_date (bool): Whether to organize summary files by date
            filename_template (str): Template for filenames
            overwrite (bool): Whether to overwrite existing summary files

        Returns:
            int: Number of videos processed
        """
        import concurrent.futures
        import time
        import os
        import logging

        logger = logging.getLogger(__name__)
        
        # Step 1: Read and chunk all transcripts first
        all_chunks = []  # Will hold (video_id, chunk_text, chunk_index, total_chunks)
        video_chunks = {}  # Maps video_id to a list of chunks

        for video in videos:
            try:
                # Read and clean the transcript
                transcript_path = video['transcript_path']
                transcript_text = self.summarizer._clean_transcript(transcript_path)

                if not transcript_text:
                    logger.warning(f"No text extracted from transcript: {transcript_path}")
                    continue

                # Use the chunk_text() function for consistent chunking with natural boundaries
                if len(transcript_text) <= self.summarizer.chunk_size:
                    # Single chunk case
                    chunks = [transcript_text]
                else:
                    logger.info(
                        f"Transcript for {video['id']} is long ({len(transcript_text)} chars), "
                        "splitting into chunks using utils.chunk_text()"
                    )
                    # Use chunk_text with overlap to ensure context is preserved between chunks
                    overlap = 100  # Characters of overlap between chunks
                    chunks = utils.chunk_text(transcript_text, self.summarizer.chunk_size, overlap)
                    logger.info(f"Successfully split into {len(chunks)} chunks using natural language boundaries")

                # Store chunks for this video
                video_chunks[video['id']] = chunks

                # Add all chunks to our flat list with their metadata
                for i, chunk in enumerate(chunks):
                    all_chunks.append((video['id'], chunk, i, len(chunks)))

                logger.info(f"Prepared {len(chunks)} chunks for video {video['id']}")

            except Exception as e:
                logger.error(f"Error preparing chunks for video {video['id']}: {e}")

        if not all_chunks:
            logger.warning("No chunks prepared for processing")
            return 0

        # Step 2: Process all chunks with a single thread pool
        chunk_results = {}  # Will hold {video_id: [chunk1_summary, chunk2_summary, ...]}

        # Calculate maximum concurrent API requests (enforcing a reasonable limit)
        actual_workers = min(self.max_parallel_workers, 5)  # Cap at 5 concurrent requests
        logger.info(f"Processing {len(all_chunks)} total chunks with {actual_workers} concurrent workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
            # Submit all chunks to the executor
            future_to_chunk = {}
            for video_id, chunk_text_data, chunk_idx, total_chunks in all_chunks:
                # Get the video object for metadata
                video = next((v for v in videos if v['id'] == video_id), None)

                future = executor.submit(
                    self.summarizer._summarize_chunk,
                    chunk_text_data, 
                    0,  # retry_count
                    3,  # max_retries
                    video.get('title', ''),
                    video.get('description', ''),
                    {'number': chunk_idx + 1, 'total': total_chunks}
                )
                future_to_chunk[future] = (video_id, chunk_idx, total_chunks)
                # Small delay between submissions
                time.sleep(delay * 0.2)

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_chunk):
                video_id, chunk_idx, total_chunks = future_to_chunk[future]
                try:
                    chunk_summary = future.result()
                    # Initialize list for this video if needed
                    if video_id not in chunk_results:
                        chunk_results[video_id] = [None] * total_chunks

                    # Store the chunk summary
                    chunk_results[video_id][chunk_idx] = chunk_summary
                    logger.info(f"Completed chunk {chunk_idx+1}/{total_chunks} for video {video_id}")

                except Exception as e:
                    logger.error(f"Error processing chunk {chunk_idx+1}/{total_chunks} for video {video_id}: {e}")

                # Small delay after each completion
                time.sleep(delay * 0.1)

        # Step 3: For each video, create a full summary by combining its chunks
        count = 0
        for video_id, summaries in chunk_results.items():
            try:
                # Filter out None results
                valid_summaries = [s for s in summaries if s]

                if not valid_summaries:
                    logger.warning(f"No valid summaries generated for video {video_id}")
                    continue

                final_summary = None

                # Get the video data for metadata
                video = next((v for v in videos if v['id'] == video_id), None)
                if not video:
                    video = self.db.get_video_with_summary(video_id) or {'id': video_id}

                # If only one chunk, use it directly
                if len(valid_summaries) == 1:
                    final_summary = valid_summaries[0]
                else:
                    # Generate meta-summary
                    logger.info(f"Creating meta-summary from {len(valid_summaries)} chunks for video {video_id}")
                    meta_prompt = f"""The following are summaries of different segments from a NH House of Representatives meeting titled "{video.get('title', '')}":
    {video.get('description', '') and f"\nDescription: {video.get('description', '')}\n" or ""}

    {"\n".join([f"Segment {i+1}/{len(valid_summaries)}: {summary}" for i, summary in enumerate(valid_summaries)])}

    Create a cohesive, comprehensive summary that combines all these segments."""

                    # Delay before meta-summary
                    time.sleep(delay)
                    try:
                        response = self.summarizer.client.chat.completions.create(
                            model=self.summarizer.model_name,
                            messages=[{"role": "user", "content": meta_prompt}],
                            max_tokens=self.summarizer.max_tokens,
                            temperature=0.7,
                            top_p=0.95,
                            timeout=120  # 2-minute timeout
                        )
                        final_summary = response.choices[0].message.content

                    except Exception as e:
                        logger.error(f"Error creating meta-summary for video {video_id}: {e}")
                        # Fall back to joining the individual summaries
                        final_summary = "## Combined Summary of Meeting Segments\n\n" + "\n\n".join(valid_summaries)

                # Save the summary to the database
                if final_summary:
                    # Immediately write the summary to a text file
                    try:
                        summary_path = self._get_summary_path(video, organize_by_date, filename_template)

                        # Create directory if it doesn't exist
                        os.makedirs(os.path.dirname(summary_path), exist_ok=True)

                        with open(summary_path, 'w', encoding='utf-8') as f:
                            f.write(f"Title: {video.get('title', 'Unknown Title')}\n")
                            f.write(f"URL: {video.get('url', 'https://www.youtube.com/watch?v=' + video_id)}\n")
                            f.write(f"Upload Date: {format_date(video.get('upload_date', ''))}\n")
                            f.write(f"Video ID: {video_id}\n\n")
                            f.write(final_summary)

                        logger.info(f"Saved summary to file: {summary_path}")

                        # Save to database with summary path
                        if self.db.save_summary(video_id, final_summary, summary_path):
                            count += 1
                            logger.info(f"Generated and saved summary for video: {video_id}")
                        else:
                            logger.warning(f"Failed to save summary to database for video: {video_id}")

                    except Exception as e:
                        logger.error(f"Error writing summary file for {video_id}: {str(e)}")
                        # Try to save to database without the file path
                        if self.db.save_summary(video_id, final_summary):
                            count += 1
                            logger.info(f"Generated and saved summary to database for video: {video_id}")
                        else:
                            logger.warning(f"Failed to save summary for video: {video_id}")
                else:
                    logger.warning(f"No valid summary generated for video: {video_id}")

            except Exception as e:
                logger.error(f"Error finalizing summary for video {video_id}: {e}")

        return count


    # Helper method to write summary files (used by both sync and async methods)
    def _write_summary_file(self, file_path, title, url, upload_date, video_id, summary):
        """
        Write a summary to a file with consistent formatting.
        
        Args:
            file_path (str): Path to write the file to
            title (str): Video title
            url (str): Video URL
            upload_date (str): Upload date string
            video_id (str): Video ID
            summary (str): Summary text
            
        Returns:
            bool: True if successful
        """
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"Title: {title}\n")
            f.write(f"URL: {url}\n")
            f.write(f"Upload Date: {format_date(upload_date)}\n")
            f.write(f"Video ID: {video_id}\n\n")
            f.write(summary)
        return True
        
    def _process_videos_async(self, videos, delay=2, organize_by_date=True, filename_template="{id}_{title}.md", 
                            overwrite=False):
        """
        Process videos using asyncio-based parallelism with a flat structure.
        
        Args:
            videos (list): List of video dictionaries with valid transcripts
            delay (int): Delay between API calls in seconds
            organize_by_date (bool): Whether to organize summary files by date
            filename_template (str): Template for filenames
            overwrite (bool): Whether to overwrite existing summary files
            
        Returns:
            int: Number of videos processed
        """
        # We need to run this async function from our sync context
        import asyncio
        import concurrent.futures
        
        async def process_all_videos():
            # Step 1: Read and chunk all transcripts first
            all_chunks = []  # Will hold (video_id, chunk_text, chunk_index, total_chunks, video)
            
            for video in videos:
                try:
                    # Read and clean the transcript (convert to async)
                    transcript_path = video['transcript_path']
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        transcript_text = await asyncio.get_event_loop().run_in_executor(
                            executor, self.summarizer._clean_transcript, transcript_path)
                    
                    if not transcript_text:
                        logger.warning(f"No text extracted from transcript: {transcript_path}")
                        continue
                        
                    # Determine if we need to chunk the transcript
                    if len(transcript_text) <= self.summarizer.chunk_size:
                        # Single chunk case
                        chunks = [transcript_text]
                    else:
                        # Multi-chunk case
                        logger.info(f"Transcript for {video['id']} is long ({len(transcript_text)} chars), splitting into chunks")
                        
                        # Use overlap to ensure context is preserved between chunks
                        overlap = 100  # Characters of overlap between chunks
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            chunks = await asyncio.get_event_loop().run_in_executor(
                                executor, utils.chunk_text, transcript_text, self.summarizer.chunk_size, overlap)
                        logger.info(f"Successfully split into {len(chunks)} chunks using natural language boundaries")
                        
                    # Add all chunks to our flat list with their metadata
                    for i, chunk in enumerate(chunks):
                        all_chunks.append((video['id'], chunk, i, len(chunks), video))
                        
                    logger.info(f"Prepared {len(chunks)} chunks for video {video['id']}")
                        
                except Exception as e:
                    logger.error(f"Error preparing chunks for video {video['id']}: {e}")
            
            if not all_chunks:
                logger.warning("No chunks prepared for processing")
                return 0
                
            # Step 2: Process all chunks with a semaphore to limit concurrency
            chunk_results = {}  # Will hold {video_id: [chunk1_summary, chunk2_summary, ...]}
            
            # Calculate maximum concurrent API requests (enforcing a reasonable limit)
            actual_workers = min(self.max_parallel_workers, 5)  # Cap at 5 concurrent requests
            semaphore = asyncio.Semaphore(actual_workers)
            logger.info(f"Processing {len(all_chunks)} total chunks with {actual_workers} concurrent workers")
            
            async def process_chunk(video_id, chunked_text, chunk_idx, total_chunks, video):
                async with semaphore:
                    # Add a small delay to avoid overwhelming the API
                    await asyncio.sleep(delay * 0.2)
                    
                    try:
                        # Convert the synchronous API call to async
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            chunk_summary = await asyncio.get_event_loop().run_in_executor(
                                executor,
                                lambda: self.summarizer._summarize_chunk(
                                    chunked_text, 
                                    0, 
                                    3,
                                    video.get('title', ''),
                                    video.get('description', ''),
                                    {'number': chunk_idx+1, 'total': total_chunks}
                                )
                            )
                        
                        # Initialize list for this video if needed
                        if video_id not in chunk_results:
                            chunk_results[video_id] = [None] * total_chunks
                            
                        # Store the chunk summary
                        chunk_results[video_id][chunk_idx] = chunk_summary
                        logger.info(f"Completed chunk {chunk_idx+1}/{total_chunks} for video {video_id}")
                        
                        return True
                    except Exception as e:
                        logger.error(f"Error processing chunk {chunk_idx+1}/{total_chunks} for video {video_id}: {e}")
                        return False
            
            # Create and gather all tasks
            tasks = []
            for video_id, chunked_text, chunk_idx, total_chunks, video in all_chunks:
                task = process_chunk(video_id, chunked_text, chunk_idx, total_chunks, video)
                tasks.append(task)
                
            # Wait for all tasks to complete
            await asyncio.gather(*tasks)
            
            # Step 3: For each video, create a full summary by combining its chunks
            count = 0
            for video_id, summaries in chunk_results.items():
                try:
                    # Filter out None results
                    valid_summaries = [s for s in summaries if s]
                    
                    if not valid_summaries:
                        logger.warning(f"No valid summaries generated for video {video_id}")
                        continue
                        
                    final_summary = None
                    
                    # Get the video for metadata
                    video = next((v for v in videos if v['id'] == video_id), None)
                    if not video:
                        # Get basic video info from database using a thread executor to avoid blocking
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            video = await asyncio.get_event_loop().run_in_executor(
                                executor, self.db.get_video_with_summary, video_id
                            ) or {'id': video_id}
                    
                    # If only one chunk, use it directly
                    if len(valid_summaries) == 1:
                        final_summary = valid_summaries[0]
                    else:
                        # Create a meta-summary from all chunks
                        logger.info(f"Creating meta-summary from {len(valid_summaries)} chunks for video {video_id}")
                        
                        meta_prompt = f"""The following are summaries of different segments from a NH House of Representatives meeting titled "{video.get('title', '')}":
{video.get('description', '') and f"\nDescription: {video.get('description', '')}\n" or ""}

{"\n".join([f"Segment {i+1}/{len(valid_summaries)}: {summary}" for i, summary in enumerate(valid_summaries)])}

Create a cohesive, comprehensive summary that combines all these segments."""

                        # Add delay before meta-summary API call
                        await asyncio.sleep(delay)
                        
                        try:
                            # Convert the synchronous API call to async
                            with concurrent.futures.ThreadPoolExecutor() as executor:
                                response = await asyncio.get_event_loop().run_in_executor(
                                    executor,
                                    lambda: self.summarizer.client.chat.completions.create(
                                        model=self.summarizer.model_name, 
                                        messages=[{"role": "user", "content": meta_prompt}], 
                                        max_tokens=self.summarizer.max_tokens, 
                                        temperature=0.7, 
                                        top_p=0.95,
                                        timeout=120  # 2-minute timeout for meta-summary
                                    )
                                )
                            
                            final_summary = response.choices[0].message.content
                            
                        except Exception as e:
                            logger.error(f"Error creating meta-summary for video {video_id}: {e}")
                            # Fall back to joining the individual summaries
                            final_summary = "## Combined Summary of Meeting Segments\n\n" + "\n\n".join(valid_summaries)
                    
                    # Save the final summary to the database and file
                    if final_summary:
                        # Write the summary to a text file
                        try:
                            summary_path = self._get_summary_path(video, organize_by_date, filename_template)
                            
                            # Create directory if it doesn't exist
                            os.makedirs(os.path.dirname(summary_path), exist_ok=True)
                            
                            # Write file using executor to avoid blocking
                            write_method = self._write_summary_file  # Create a reference to the method
                            with concurrent.futures.ThreadPoolExecutor() as executor:
                                await asyncio.get_event_loop().run_in_executor(
                                    executor,
                                    lambda: write_method(
                                        summary_path, 
                                        video.get('title', 'Unknown Title'),
                                        video.get('url', 'https://www.youtube.com/watch?v=' + video_id),
                                        video.get('upload_date', ''),
                                        video_id,
                                        final_summary
                                    )
                                )
                            
                            logger.info(f"Saved summary to file: {summary_path}")
                            
                            # Save to database with summary path
                            with concurrent.futures.ThreadPoolExecutor() as executor:
                                success = await asyncio.get_event_loop().run_in_executor(
                                    executor, self.db.save_summary, video_id, final_summary, summary_path
                                )
                                
                            if success:
                                count += 1
                                logger.info(f"Generated and saved summary for video: {video_id}")
                            else:
                                logger.warning(f"Failed to save summary to database for video: {video_id}")
                                
                        except Exception as e:
                            logger.error(f"Error writing summary file for {video_id}: {str(e)}")
                            # Try to save to database without the file path
                            with concurrent.futures.ThreadPoolExecutor() as executor:
                                success = await asyncio.get_event_loop().run_in_executor(
                                    executor, self.db.save_summary, video_id, final_summary
                                )
                                
                            if success:
                                count += 1
                                logger.info(f"Generated and saved summary to database for video: {video_id}")
                            else:
                                logger.warning(f"Failed to save summary for video: {video_id}")
                    else:
                        logger.warning(f"No valid summary generated for video: {video_id}")
                        
                except Exception as e:
                    logger.error(f"Error finalizing summary for video {video_id}: {e}")
            
            return count
        
        # Run the async function and get the result
        return asyncio.run(process_all_videos())
    
    def export_summaries(self, output_dir=None, organize_by_date=True, limit=None, 
                          filename_template="{id}_{title}.md", overwrite=False, query=None):
        """
        Export summaries to text files in an organized directory structure.
        
        Args:
            output_dir (str, optional): Base directory for exporting summaries (defaults to self.output_dir)
            organize_by_date (bool): Whether to organize by date in YYYY/MM subdirectories
            limit (int, optional): Maximum number of summaries to export
            filename_template (str): Template for filenames
            overwrite (bool): Whether to overwrite existing files
            query (str, optional): Search query to filter videos
            
        Returns:
            tuple: (Number of summaries exported, List of exported file paths)
        """
        if output_dir is None:
            output_dir = self.output_dir
            
        logger.info(f"Exporting summaries to {output_dir}")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Get videos with summaries
        if query:
            videos = self.search_videos(query, limit=limit or 1000)
        else:
            videos = self.db.get_videos_with_summaries(limit=limit)
        
        if not videos:
            logger.warning("No videos with summaries found")
            return 0, []
            
        count = 0
        exported_files = []
        
        for video in videos:
            if not video.get('summary'):
                logger.debug(f"No summary for video: {video['id']}")
                continue
                
            # Set output_dir temporarily to the specified directory
            original_output_dir = self.output_dir
            self.output_dir = output_dir
            
            # Get the summary path
            output_path = self._get_summary_path(video, organize_by_date, filename_template)
            
            # Restore the original output directory
            self.output_dir = original_output_dir
                
            # Skip if file exists and overwrite is False
            if os.path.exists(output_path) and not overwrite:
                logger.info(f"Skipping existing file: {output_path}")
                continue
                
            try:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
                # Write summary to file
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(f"Title: {video['title']}\n")
                    f.write(f"URL: {video['url']}\n")
                    f.write(f"Upload Date: {format_date(video['upload_date'])}\n")
                    f.write(f"Video ID: {video['id']}\n\n")
                    f.write(video['summary'])
                    
                count += 1
                exported_files.append(output_path)
                logger.info(f"Exported summary to {output_path}")
                
            except Exception as e:
                logger.error(f"Error exporting summary for {video['id']}: {e}")
                
        logger.info(f"Exported {count} summaries to {output_dir}")
        return count, exported_files