# NH House of Representatives Video Summarizer

A Python package that downloads, processes, and summarizes videos from the NH House of Representatives YouTube channel using yt-dlp and the Hyperbolic API.

## Features

- Download video metadata from the New Hampshire House of Representatives YouTube channel
- Extract transcripts (subtitles) from videos
- Summarize video content using the Hyperbolic API
- Store videos and summaries in a SQLite database
- Search for videos by title, description, or summary content
- Command-line interface for easy interaction

## Installation

```bash
pip install git+https://github.com/proximile/NH-House-of-Representatives-Committee-Summaries
```

## Configuration

You'll need an API key from [Hyperbolic](https://hyperbolic.xyz) to use the summarization features. You can set your API key in one of these ways:

1. Environment variable:
   ```bash
   export HYPERBOLIC_API_KEY='your-api-key-here'
   ```

2. Pass it directly to the command:
   ```bash
   nhvideos --api-key 'your-api-key-here' process
   ```

3. Store it in a file and reference it:
   ```bash
   echo 'your-api-key-here' > hyperbolic_api_key.txt
   nhvideos --api-key-file hyperbolic_api_key.txt process
   ```

## Usage

### Command-line interface

The package provides a command-line tool `nh_house_of_reps_summarizer` with several subcommands:

#### Fetch videos from the channel

```bash
# Fetch the latest videos (up to a limit)
nh_house_of_reps_summarizer fetch --limit 50
```

#### Download transcripts

```bash
# Download transcripts for videos in the database
nh_house_of_reps_summarizer transcripts --limit 50
```

#### Process videos and generate summaries

```bash
# Process unprocessed videos and generate summaries
nh_house_of_reps_summarizer process --limit 5 --delay 2
```

#### Run the full pipeline

```bash
# Run everything: fetch videos, download transcripts, generate summaries
nh_house_of_reps_summarizer pipeline --fetch-limit 50 --process-limit 10 --delay 2
```

#### Search for videos

```bash
# Search for videos by title, description, or summary
nh_house_of_reps_summarizer search "budget"
```

#### View video details

```bash
# View details and summary for a specific video
nh_house_of_reps_summarizer info VIDEO_ID
```

#### Extract summary to a file

```bash
# Extract a summary to a file
nh_house_of_reps_summarizer extract VIDEO_ID --output summary.txt
```

### Python API

You can also use the package programmatically:

```python
from nh_house_of_reps_summarizer import NHVideoProcessor

# Initialize the processor
processor = NHVideoProcessor(
    api_key='your-api-key-here',
    download_dir='downloads',
    db_path='nh_videos.db'
)

# Fetch videos
processor.fetch_channel_videos(limit=50)

# Download transcripts
processor.download_transcripts(limit=50)

# Process videos and generate summaries
processor.process_videos(limit=5, delay=2)

# Search for videos
results = processor.search_videos('budget', limit=10)
for video in results:
    print(f"{video['title']} - {video['url']}")
    
# Get details for a specific video
video = processor.get_video_summary('VIDEO_ID')
if video and 'summary' in video:
    print(video['summary'])
```

## Requirements

- Python 3.9 or higher
- yt-dlp
- openai
- beautifulsoup4
- requests
- SQLite

## License

This project is licensed under the MIT License - see the LICENSE file for details.
