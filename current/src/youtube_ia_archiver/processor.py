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
        self.ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": False,  # We DO need media here
            "writeinfojson": True,
            "noplaylist": True,
        }

        # 2. Merge Global YAML options (Quality, Subs, Sleep)
        self.ydl_opts.update(self.config.global_ydl_opts)

        # 3. Apply Authentication
        cookie_path = self.config.ydl_cookie_file
        if cookie_path and os.path.exists(cookie_path):
            self.ydl_opts["cookiefile"] = os.path.abspath(cookie_path)
            logger.info(f"Using shared cookies from: {cookie_path}")

    def get_playlist_video_ids(self) -> list:
        """Scan playlist and extract unique video IDs."""
        scan_opts = self.ydl_opts.copy()
        scan_opts.update({"extract_flat": "in_playlist", "skip_download": True})

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

        # Load description prefix template
        template_path = (
            Path(os.getcwd()) / self.config.raw["ia_settings"]["description_template"]
        )
        prefix = (
            template_path.read_text(encoding="utf-8") if template_path.exists() else ""
        )

        return {
            "title": title,
            "description": f"{prefix}\n\n{info.get('description', '')}",
            "mediatype": "movies",
            "collection": self.config.get("ia_settings", "collection"),
            "external-identifier": f"youtube:{info['id']}",
            "originalurl": f"https://www.youtube.com/watch?v={info['id']}",
            "creator": info.get("uploader", "Unknown"),
        }

    def process_video(self, video_id: str, dry_run: bool = False) -> bool:
        """Executes the full archival pipeline for a single video."""
        work_dir = self.config.temp_work_dir / video_id
        if dry_run:
            logger.info(f"[Dry-run] Would archive video: {video_id}")
            return True

        work_dir.mkdir(parents=True, exist_ok=True)
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        try:
            # 1. Download media and assets
            local_opts = self.ydl_opts.copy()
            local_opts["outtmpl"] = f"{work_dir}/%(title)s.%(ext)s"

            with yt_dlp.YoutubeDL(local_opts) as ydl:
                info = ydl.extract_info(video_url, download=True)
                title = info.get("title", "Unknown Title")

            # 2. Rename info.json to Title.json as requested
            info_json = work_dir / f"{title}.info.json"
            if info_json.exists():
                shutil.move(info_json, work_dir / f"{title}.json")

            # 3. Load IA Credentials
            ia_creds = {}
            if self.config.credentials_file.exists():
                for line in self.config.credentials_file.read_text().splitlines():
                    if "=" in line:
                        k, v = line.strip().split("=", 1)
                        ia_creds[k] = v.strip('"').strip("'")

            # 4. Upload to IA
            logger.info(f"Uploading assets to Internet Archive. Identifier: {video_id}")
            files = [str(f) for f in work_dir.iterdir() if f.is_file()]

            responses = upload(
                identifier=video_id,
                files=files,
                metadata=self._prepare_metadata(info),
                access_key=ia_creds.get("IA_ACCESS_KEY"),
                secret_key=ia_creds.get("IA_SECRET_KEY"),
                verbose=True,
            )

            if all(r.status_code == 200 for r in responses):
                logger.info(f"Successful archival of {video_id}")
                return True

            return False

        except Exception as e:
            logger.error(f"Pipeline error for {video_id}: {e}")
            return False
        finally:
            # 5. Immediate cleanup of local media files
            if work_dir.exists():
                logger.info(f"Purging temporary workspace: {work_dir}")
                shutil.rmtree(work_dir)
