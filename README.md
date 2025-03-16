# NH House of Representatives Video Summarizer

A Python application that downloads, processes, and summarizes videos from the NH House of Representatives YouTube channel using yt-dlp and the Hyperbolic API.

## Features

- Download video metadata from the New Hampshire House of Representatives YouTube channel
- Extract transcripts (subtitles) from videos
- Summarize video content using the Hyperbolic API
- Store videos and summaries in a SQLite database
- Search for videos by title, description, or summary content
- Command-line interface for easy interaction

## Getting Started

### Prerequisites

- Python 3.9 or higher
- Dependencies:
  - yt-dlp
  - openai
  - beautifulsoup4
  - requests
  - SQLite

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/proximile/NH-House-of-Representatives-Committee-Summaries
   cd NH-House-of-Representatives-Committee-Summaries
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Configuration

You'll need an API key from [Hyperbolic](https://hyperbolic.xyz) to use the summarization features. You can set your API key in one of these ways:

1. Environment variable:
   ```bash
   export HYPERBOLIC_API_KEY='your-api-key-here'
   ```

2. Pass it directly to the command:
   ```bash
   python -m nh_house_of_reps_summarizer.run --api-key 'your-api-key-here' process
   ```

3. Store it in a file and reference it:
   ```bash
   echo 'your-api-key-here' > hyperbolic_api_key.txt
   python -m nh_house_of_reps_summarizer.run --api-key-file hyperbolic_api_key.txt process
   ```

## Usage

### Command-line interface

The application provides a command-line interface with several subcommands:

#### Fetch videos from the channel
```bash
python -m nh_house_of_reps_summarizer.run fetch --limit 50
```

#### Download transcripts
```bash
python -m nh_house_of_reps_summarizer.run transcripts --limit 50
```

#### Process videos and generate summaries
```bash
python -m nh_house_of_reps_summarizer.run process --limit 5 --delay 2
```

#### Run the full pipeline
```bash
python -m nh_house_of_reps_summarizer.run pipeline --fetch-limit 50 --process-limit 10 --delay 2
```

#### Search for videos
```bash
python -m nh_house_of_reps_summarizer.run search "budget"
```

#### View video details
```bash
python -m nh_house_of_reps_summarizer.run info VIDEO_ID
```

#### Extract summary to a file
```bash
python -m nh_house_of_reps_summarizer.run extract VIDEO_ID --output summary.txt
```

### Python API

You can also use the application programmatically:

```python
from nh_house_of_reps_summarizer.main import NHVideoProcessor

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

## Project Structure

```
NH-House-of-Representatives-Committee-Summaries/
├── README.md
├── data/
└── nh_house_of_reps_summarizer/
    ├── __init__.py
    ├── audio_transcriber.py
    ├── cli.py
    ├── cli_commands.py
    ├── database.py
    ├── main.py
    ├── run.py
    ├── summarizer.py
    ├── utils.py
    └── video_downloader.py
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.