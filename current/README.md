# DaGhE YouTube Archiver (IA & Wayback)

This module provides a professional-grade redundant archival pipeline. It monitors YouTube playlists and synchronises new content to **Internet Archive** (media and metadata) and **The Wayback Machine** (URL snapshots).

## 🏗 Core Strategy

The module operates in a sequential, high-reliability pipeline:

1.  **Playlist Synchronisation**: 
    *   Scans the configured playlist and saves a standardised `playlist_<id>.json` to the `data/` directory.
    *   Filters out videos already recorded in `archived_videos.txt`.
2.  **Asset Extraction**:
    *   Downloads media in maximum quality (standardised to `.mp4` via FFmpeg).
    *   Extracts subtitles (English/Italian) and full metadata.
3.  **Redundant Archival**:
    *   **Wayback Machine**: Triggers a 'Save Page Now' request for the YouTube URL.
    *   **Internet Archive**: Creates a new item using the YouTube ID as a bucket (prefixed with `yt-`).
4.  **Metadata Enrichment**:
    *   The IA description is generated using a custom local template (`description_prefix.txt`) with support for dynamic placeholders (`{date}`, `{uploader}`, etc.).
    *   A detailed TSV registry is updated with permanent archival links.

## ⚙️ Configuration (`job.yaml`)

### Throttling & Reliability
API rate limits are managed via the `timeouts` section:
*   `ia_upload`: Polling ensures the item is indexed before the script marks it as success.
*   `wayback`: Implements retries and socket timeouts to handle slow archival requests.

### Inventory Registry
All successful archives are logged in a TSV file (`inventory_tsv`) containing:
*   Original YouTube ID and Title.
*   Internet Archive Identifier and Direct URL.
*   Wayback Machine Snapshot URL.

## 🚀 Usage via DaGhE

### Installation
```bash
uv run bin/daghe install daghe-youtube-ia-archiver
```

### Manual Run (Testing)
```bash
uv run bin/daghe run daghe-youtube-ia-archiver
```

### Secrets Management
S3 keys for Archive.org must be stored in `${BASE_DIR}/config/ia.env`.

## 🛠 Prerequisites
*   **FFmpeg**: Required for merging video and audio streams into MP4.
*   **Deno**: Required for bypassing YouTube's crptographic challenges.
*   **waybackpy**: Used for the Wayback Machine integration.
