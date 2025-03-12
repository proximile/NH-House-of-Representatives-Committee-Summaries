"""
Database management for NH House videos and summaries.
"""

import sqlite3
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Class for managing the database of videos and summaries.
    """
    
    def __init__(self, db_path="nh_videos.db"):
        """
        Initialize the DatabaseManager.
        
        Args:
            db_path (str): Path to the SQLite database file
        """
        self.db_path = db_path
        self._create_tables()
        
    def _create_tables(self):
        """Create necessary tables if they don't exist."""
        logger.info("Initializing database tables")
        
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Videos table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                id TEXT PRIMARY KEY,
                title TEXT,
                description TEXT,
                upload_date TEXT,
                duration INTEGER,
                view_count INTEGER,
                url TEXT,
                transcript_path TEXT,
                processed INTEGER DEFAULT 0,
                has_subtitles INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            # Summaries table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS summaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT,
                summary TEXT,
                summary_path TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (video_id) REFERENCES videos (id)
            )
            ''')
            
            # Check if summary_path column exists, add it if not
            cursor.execute("PRAGMA table_info(summaries)")
            columns = cursor.fetchall()
            column_names = [column[1] for column in columns]
            
            if 'summary_path' not in column_names:
                cursor.execute("ALTER TABLE summaries ADD COLUMN summary_path TEXT")
                logger.info("Added summary_path column to summaries table")
            
            # Add indices for faster queries
            cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_video_processed ON videos (processed);
            ''')
            
            cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_summaries_video_id ON summaries (video_id);
            ''')
            
            conn.commit()
            logger.info("Database tables initialized")
            
        except Exception as e:
            logger.error(f"Error creating database tables: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
        
    def add_video(self, video_metadata, transcript_path=None):
        """
        Add a video to the database or update if it already exists.
        
        Args:
            video_metadata (dict): Video metadata
            transcript_path (str, optional): Path to the transcript file
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not video_metadata or 'id' not in video_metadata:
            logger.error("Invalid video metadata")
            return False
        
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if the video already exists
            cursor.execute("SELECT id FROM videos WHERE id = ?", (video_metadata['id'],))
            exists = cursor.fetchone()
            
            has_subtitles = 1 if video_metadata.get('has_subtitles', False) else 0
            
            if exists:
                # Update existing video
                cursor.execute('''
                UPDATE videos SET
                    title = ?,
                    description = ?,
                    upload_date = ?,
                    duration = ?,
                    view_count = ?,
                    url = ?,
                    transcript_path = COALESCE(?, transcript_path),
                    has_subtitles = ?
                WHERE id = ?
                ''', (
                    video_metadata.get('title'),
                    video_metadata.get('description'),
                    video_metadata.get('upload_date'),
                    video_metadata.get('duration'),
                    video_metadata.get('view_count'),
                    video_metadata.get('url'),
                    transcript_path,
                    has_subtitles,
                    video_metadata['id']
                ))
                logger.info(f"Updated video in database: {video_metadata['id']}")
            else:
                # Insert new video
                cursor.execute('''
                INSERT INTO videos (
                    id, title, description, upload_date, duration, view_count, 
                    url, transcript_path, has_subtitles, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    video_metadata['id'],
                    video_metadata.get('title'),
                    video_metadata.get('description'),
                    video_metadata.get('upload_date'),
                    video_metadata.get('duration'),
                    video_metadata.get('view_count'),
                    video_metadata.get('url'),
                    transcript_path,
                    has_subtitles,
                    datetime.now().isoformat()
                ))
                logger.info(f"Added new video to database: {video_metadata['id']}")
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error adding video to database: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def update_transcript_path(self, video_id, transcript_path):
        """
        Update the transcript path for a video.
        
        Args:
            video_id (str): Video ID
            transcript_path (str): Path to the transcript file
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not video_id or not transcript_path:
            return False
        
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            UPDATE videos SET transcript_path = ? WHERE id = ?
            ''', (transcript_path, video_id))
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error updating transcript path: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def get_unprocessed_videos(self, limit=10):
        """
        Get videos that have not been processed yet.
        
        Args:
            limit (int): Maximum number of videos to retrieve
            
        Returns:
            list: List of video dictionaries
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get videos that don't have entries in the summaries table
            cursor.execute('''
            SELECT v.* FROM videos v
            LEFT JOIN summaries s ON v.id = s.video_id
            WHERE s.video_id IS NULL AND v.transcript_path IS NOT NULL 
            ORDER BY v.upload_date DESC LIMIT ?
            ''', (limit,))
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting unprocessed videos: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_videos_with_summaries(self, limit=None):
        """
        Get all videos that have summaries.
        
        Args:
            limit (int, optional): Maximum number of videos to retrieve
            
        Returns:
            list: List of video dictionaries with summaries
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = '''
            SELECT v.*, s.summary, s.summary_path 
            FROM videos v
            JOIN summaries s ON v.id = s.video_id
            ORDER BY v.upload_date DESC
            '''
            
            if limit:
                query += f" LIMIT {limit}"
                
            cursor.execute(query)
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Error getting videos with summaries: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    def mark_video_processed(self, video_id, processed=True):
        """
        Mark a video as processed.
        
        Args:
            video_id (str): Video ID
            processed (bool): Whether the video is processed
            
        Returns:
            bool: True if successful, False otherwise
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            processed_value = 1 if processed else 0
            cursor.execute('''
            UPDATE videos SET processed = ? WHERE id = ?
            ''', (processed_value, video_id))
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error marking video as processed: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def save_summary(self, video_id, summary, summary_path=None):
        """
        Save a summary for a video.
        
        Args:
            video_id (str): Video ID
            summary (str): Generated summary
            summary_path (str, optional): Path to the saved summary file
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not video_id or not summary:
            return False
        
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if a summary already exists
            cursor.execute("SELECT id FROM summaries WHERE video_id = ?", (video_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing summary
                if summary_path:
                    cursor.execute('''
                    UPDATE summaries SET summary = ?, summary_path = ?, created_at = ? WHERE video_id = ?
                    ''', (summary, summary_path, datetime.now().isoformat(), video_id))
                else:
                    cursor.execute('''
                    UPDATE summaries SET summary = ?, created_at = ? WHERE video_id = ?
                    ''', (summary, datetime.now().isoformat(), video_id))
            else:
                # Insert new summary
                if summary_path:
                    cursor.execute('''
                    INSERT INTO summaries (video_id, summary, summary_path, created_at) VALUES (?, ?, ?, ?)
                    ''', (video_id, summary, summary_path, datetime.now().isoformat()))
                else:
                    cursor.execute('''
                    INSERT INTO summaries (video_id, summary, created_at) VALUES (?, ?, ?)
                    ''', (video_id, summary, datetime.now().isoformat()))
            
            # Mark the video as processed
            cursor.execute('''
            UPDATE videos SET processed = 1 WHERE id = ?
            ''', (video_id,))
            
            conn.commit()
            logger.info(f"Saved summary for video: {video_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving summary: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def update_summary_path(self, video_id, summary_path):
        """
        Update the summary file path for a video.
        
        Args:
            video_id (str): Video ID
            summary_path (str): Path to the summary file
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not video_id or not summary_path:
            return False
        
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            UPDATE summaries SET summary_path = ? WHERE video_id = ?
            ''', (summary_path, video_id))
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error updating summary path: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()
    
    def get_video_with_summary(self, video_id):
        """
        Get a video and its summary.
        
        Args:
            video_id (str): Video ID
            
        Returns:
            dict: Video data including summary, or None if not found
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT v.*, s.summary, s.summary_path 
            FROM videos v
            LEFT JOIN summaries s ON v.id = s.video_id
            WHERE v.id = ?
            ''', (video_id,))
            
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
            
        except Exception as e:
            logger.error(f"Error getting video with summary: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def search_videos(self, query, limit=20):
        """
        Search for videos by title, description, or summary.
        
        Args:
            query (str): Search query
            limit (int): Maximum number of results to return
            
        Returns:
            list: List of video dictionaries
        """
        if not query:
            return []
        
        search_term = f"%{query}%"
        
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT v.*, s.summary, s.summary_path 
            FROM videos v
            LEFT JOIN summaries s ON v.id = s.video_id
            WHERE v.title LIKE ? OR v.description LIKE ? OR s.summary LIKE ?
            ORDER BY v.upload_date DESC
            LIMIT ?
            ''', (search_term, search_term, search_term, limit))
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Error searching videos: {e}")
            return []
        finally:
            if conn:
                conn.close()
                
    def get_summary_stats(self):
        """
        Get statistics about summaries in the database.
        
        Returns:
            dict: Dictionary with summary statistics
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Total videos
            cursor.execute("SELECT COUNT(*) FROM videos")
            total_videos = cursor.fetchone()[0]
            
            # Videos with summaries
            cursor.execute("SELECT COUNT(*) FROM summaries")
            total_summaries = cursor.fetchone()[0]
            
            # Videos with transcripts
            cursor.execute("SELECT COUNT(*) FROM videos WHERE transcript_path IS NOT NULL")
            total_transcripts = cursor.fetchone()[0]
            
            # Videos pending processing
            cursor.execute("""
                SELECT COUNT(*) FROM videos v
                LEFT JOIN summaries s ON v.id = s.video_id
                WHERE v.transcript_path IS NOT NULL 
                AND s.video_id IS NULL
            """)
            pending_processing = cursor.fetchone()[0]
            
            # Videos grouped by month
            cursor.execute("""
                SELECT substr(upload_date, 1, 6) as month, COUNT(*) as count
                FROM videos
                GROUP BY month
                ORDER BY month DESC
                LIMIT 12
            """)
            months = cursor.fetchall()
            
            # Videos with summary files
            cursor.execute("""
                SELECT COUNT(*) FROM summaries 
                WHERE summary_path IS NOT NULL
            """)
            total_summary_files = cursor.fetchone()[0]
            
            return {
                'total_videos': total_videos,
                'total_summaries': total_summaries,
                'total_transcripts': total_transcripts,
                'pending_processing': pending_processing,
                'total_summary_files': total_summary_files,
                'months': {month: count for month, count in months}
            }
            
        except Exception as e:
            logger.error(f"Error getting summary stats: {e}")
            return {
                'total_videos': 0,
                'total_summaries': 0,
                'total_transcripts': 0,
                'pending_processing': 0,
                'total_summary_files': 0,
                'months': {}
            }
        finally:
            if conn:
                conn.close()