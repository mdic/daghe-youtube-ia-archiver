import json
import logging
import os
import shutil
from pathlib import Path

import yt_dlp
from internetarchive import upload

logger = logging.getLogger(__name__)


class ArchiveProcessor:
    def __init__(self, config):
        """
        Initialise the processor with yt-dlp authenticated session.
        UK English spelling: Specialised for Internet Archive pipelines.
        """
        self.config = config

        # 1. Core yt-dlp Configuration
        # We start with basic sensible defaults for a scraper.
        self.ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": False,
            "writeinfojson": True,
            "noplaylist": True,
            "extract_flat": False,  # Ensure we extract formats by default
        }

        # 2. Merge Global YAML options (Quality, Subs, Sleep, etc.)
        # This will include 'format', 'subtitleslangs', etc., from job.yaml
        global_extras = self.config.global_ydl_opts
        if global_extras:
            self.ydl_opts.update(global_extras)

        # 3. Apply Authentication
        cookie_path = self.config.ydl_cookie_file
        if cookie_path and os.path.exists(cookie_path):
            self.ydl_opts["cookiefile"] = os.path.abspath(cookie_path)
            logger.info(f"Using shared cookies from: {cookie_path}")

    def get_playlist_video_ids(self) -> list:
        """Scan playlist and extract unique video IDs."""
        # For scanning, we override to 'flat' mode to be fast and avoid format errors
        scan_opts = self.ydl_opts.copy()
        scan_opts.update(
            {
                "extract_flat": "in_playlist",
                "skip_download": True,
                "ignore_no_formats_error": True,
            }
        )

        logger.info(f"Scanning playlist: {self.config.playlist_url}")
        try:
            with yt_dlp.YoutubeDL(scan_opts) as ydl:
                result = ydl.extract_info(self.config.playlist_url, download=False)
                return [e["id"] for e in result.get("entries", []) if e.get("id")]
        except Exception as e:
            logger.error(f"Playlist extraction failed: {e}")
            return []

    def _prepare_metadata(self, info: dict) -> dict:
        """Construct the Internet Archive metadata schema."""
        title = info.get("title", "Unknown Title")
        video_id = info.get("id")

        # Load description prefix template from module config directory
        # We resolve it relative to the current working directory (module root)
        template_path = (
            Path(os.getcwd()) / self.config.raw["ia_settings"]["description_template"]
        )

        prefix = ""
        if template_path.exists():
            prefix = template_path.read_text(encoding="utf-8")
        else:
            logger.warning(f"Description template not found at: {template_path}")

        return {
            "title": title,
            "description": f"{prefix}\n\n{info.get('description', '')}",
            "mediatype": "movies",
            "collection": self.config.get("ia_settings", "collection"),
            "external-identifier": f"youtube:{video_id}",
            "originalurl": f"https://www.youtube.com/watch?v={video_id}",
            "creator": info.get("uploader", "Unknown"),
        }

    def process_video(self, video_id: str, dry_run: bool = False) -> bool:
        """Executes the full archival pipeline for a single video."""
        work_dir = self.config.temp_work_dir / video_id
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        if dry_run:
            logger.info(f"[Dry-run] Would archive video: {video_id}")
            return True

        if work_dir.exists():
            shutil.rmtree(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            # 1. Download media and assets
            # We ensure extract_flat is False here so yt-dlp looks for real video streams
            local_opts = self.ydl_opts.copy()
            local_opts["extract_flat"] = False
            local_opts["outtmpl"] = f"{work_dir}/%(title)s.%(ext)s"

            logger.info(
                f"DEBUG - Initialising yt-dlp with the following options for {video_id}:"
            )
            # Log the options in JSON format for readability (excluding binary data if present)
            logger.info(
                json.dumps({k: str(v) for k, v in local_opts.items()}, indent=4)
            )

            logger.info(f"Downloading assets for {video_id} (URL: {video_url})...")
            with yt_dlp.YoutubeDL(local_opts) as ydl:
                # We extract and download in one step
                info = ydl.extract_info(video_url, download=True)
                title = info.get("title", "Unknown Title")

            # 2. Rename info.json to Title.json as requested
            # yt-dlp saves info.json alongside the video using outtmpl
            info_json = work_dir / f"{title}.info.json"
            if info_json.exists():
                shutil.move(str(info_json), str(work_dir / f"{title}.json"))
            else:
                logger.warning(f"Could not find info.json to rename for {video_id}")

            # 3. Load IA Credentials from the configured ia.env
            ia_creds = {}
            creds_file = self.config.credentials_file
            if creds_file.exists():
                for line in creds_file.read_text().splitlines():
                    if "=" in line and not line.startswith("#"):
                        k, v = line.strip().split("=", 1)
                        ia_creds[k] = v.strip('"').strip("'")
            else:
                logger.error(f"IA credentials file missing at: {creds_file}")
                return False

            # 4. Upload to Internet Archive
            logger.info(f"Uploading assets to Internet Archive. Identifier: {video_id}")
            files_to_upload = [str(f) for f in work_dir.iterdir() if f.is_file()]

            if not files_to_upload:
                logger.error(f"No files found in {work_dir} to upload.")
                return False

            responses = upload(
                identifier=video_id,
                files=files_to_upload,
                metadata=self._prepare_metadata(info),
                access_key=ia_creds.get("IA_ACCESS_KEY"),
                secret_key=ia_creds.get("IA_SECRET_KEY"),
                verbose=True,
            )

            # Check if all files were accepted by IA
            if all(r.status_code == 200 for r in responses):
                logger.info(f"Successful archival of {video_id}")
                return True
            else:
                logger.error(f"Some upload responses for {video_id} were unsuccessful.")
                return False

        except Exception as e:
            logger.error(f"Pipeline error for {video_id}: {e}")
            return False
        finally:
            # 5. Immediate cleanup of local media files to save VPS disk space
            if work_dir.exists():
                logger.info(f"Purging temporary workspace: {work_dir}")
                shutil.rmtree(work_dir)
