import logging
import os
import shutil
import subprocess
from pathlib import Path

import yt_dlp

logger = logging.getLogger(__name__)


class ArchiveProcessor:
    def __init__(self, config):
        self.config = config

    def get_playlist_video_ids(self) -> list:
        """Extract video IDs from the playlist using a flat scan."""
        ydl_opts = {"extract_flat": True, "quiet": True}
        logger.info(f"Scanning playlist: {self.config.playlist_url}")
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                result = ydl.extract_info(self.config.playlist_url, download=False)
                return [
                    entry["id"]
                    for entry in result.get("entries", [])
                    if entry.get("id")
                ]
        except Exception as e:
            logger.error(f"Failed to scan playlist: {e}")
            return []

    def process_video(self, video_id: str, dry_run: bool = False) -> bool:
        """Download and upload a video to IA using tubeup."""
        if dry_run:
            logger.info(f"[Dry-run] Would archive video: {video_id}")
            return True

        work_dir = self.config.temp_work_dir / video_id
        work_dir.mkdir(parents=True, exist_ok=True)

        # Load IA credentials from env file for this subprocess
        env = os.environ.copy()
        if self.config.credentials_file.exists():
            with open(self.config.credentials_file) as f:
                for line in f:
                    if "=" in line:
                        k, v = line.strip().split("=", 1)
                        env[k] = v.strip('"').strip("'")

        video_url = f"https://www.youtube.com/watch?v={video_id}"
        logger.info(f"Starting TubeUp for: {video_id}")

        try:
            # We call tubeup via subprocess to ensure clean memory management
            # and easy handling of its internal temp files.
            cmd = [
                "tubeup",
                "--metadata",
                f"collection:{self.config.get('ia_settings', 'collection')}",
                "--workdir",
                str(work_dir),
                video_url,
            ]

            result = subprocess.run(cmd, env=env, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info(f"Successfully archived {video_id} to IA.")
                return True
            else:
                logger.error(f"TubeUp failed for {video_id}: {result.stderr}")
                return False
        finally:
            # UK English: Purge local media files immediately to save disk space.
            if work_dir.exists():
                shutil.rmtree(work_dir)
