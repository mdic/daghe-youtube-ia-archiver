import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

import yt_dlp
from internetarchive import upload

logger = logging.getLogger(__name__)


class YdlLogger:
    def debug(self, msg):
        if msg.startswith("[debug] "):
            pass
        else:
            logger.debug(msg)

    def info(self, msg):
        logger.info(msg)

    def warning(self, msg):
        logger.warning(msg)

    def error(self, msg):
        logger.error(msg)


class ArchiveProcessor:
    def __init__(self, config):
        """Initialise with robust session options and UK English logging."""
        self.config = config
        self.ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": False,
            "writeinfojson": True,
            "noplaylist": True,
            "extract_flat": False,
            "logger": YdlLogger(),
        }
        global_extras = self.config.global_ydl_opts
        if global_extras:
            self._apply_extra_opts(global_extras)

        cookie_path = self.config.ydl_cookie_file
        if cookie_path and os.path.exists(cookie_path):
            self.ydl_opts["cookiefile"] = os.path.abspath(cookie_path)

    def _apply_extra_opts(self, extras: dict):
        for k, v in extras.items():
            if isinstance(v, str):
                if v.lower() == "true":
                    v = True
                elif v.lower() == "false":
                    v = False
            self.ydl_opts[k] = v

    def get_playlist_video_ids(self) -> list:
        scan_opts = self.ydl_opts.copy()
        scan_opts.update(
            {
                "extract_flat": "in_playlist",
                "skip_download": True,
                "ignore_no_formats_error": True,
            }
        )
        try:
            with yt_dlp.YoutubeDL(scan_opts) as ydl:
                result = ydl.extract_info(self.config.playlist_url, download=False)
                return [e["id"] for e in result.get("entries", []) if e.get("id")]
        except Exception as e:
            logger.error(f"Playlist scan failed: {e}")
            return []

    def _prepare_description(self, info: dict) -> str:
        """
        Constructs the final description using the external template and YouTube metadata.
        UK English: Handles placeholder substitution for the Internet Archive metadata field.
        """
        template_path = (
            Path(os.getcwd()) / self.config.raw["ia_settings"]["description_template"]
        )

        # Extract metadata for placeholders
        metadata_context = {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "title": info.get("title", "N/A"),
            "description": info.get("description", "No description available."),
            "uploader": info.get("uploader", "Unknown Account"),
            "likes": info.get("like_count", "N/A"),
        }

        if template_path.exists():
            try:
                template_text = template_path.read_text(encoding="utf-8")
                # We use .format(**metadata_context) to map all dictionary keys to placeholders
                return template_text.format(**metadata_context)
            except KeyError as e:
                logger.error(f"Missing placeholder in description_prefix.txt: {e}")
                return metadata_context["description"]

        return metadata_context["description"]

    def process_video(
        self, video_id: str, dry_run: bool = False
    ) -> tuple[bool, dict | None]:
        work_dir = self.config.temp_work_dir / video_id
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        if dry_run:
            logger.info(f"[Dry-run] Archival simulation for: {video_id}")
            return True, None

        if work_dir.exists():
            shutil.rmtree(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            local_opts = self.ydl_opts.copy()
            local_opts["outtmpl"] = f"{work_dir}/%(title)s.%(ext)s"

            logger.info(f"Initiating archival for {video_id}...")
            with yt_dlp.YoutubeDL(local_opts) as ydl:
                info = ydl.extract_info(video_url, download=True)
                title = info.get("title", "Unknown Title")

            # Rename info.json to [Title].json
            info_json = work_dir / f"{title}.info.json"
            if info_json.exists():
                shutil.move(str(info_json), str(work_dir / f"{title}.json"))

            # Load IA Credentials
            ia_creds = {}
            if self.config.credentials_file.exists():
                for line in self.config.credentials_file.read_text().splitlines():
                    if "=" in line and not line.startswith("#"):
                        k, v = line.strip().split("=", 1)
                        ia_creds[k] = v.strip('"').strip("'")

            # Upload to IA
            logger.info(f"Uploading assets to Internet Archive: {video_id}")
            files_to_upload = [str(f) for f in work_dir.iterdir() if f.is_file()]

            ia_metadata = {
                "title": title,
                "description": self._prepare_description(info),
                "mediatype": "movies",
                "collection": self.config.get("ia_settings", "collection"),
                "external-identifier": f"youtube:{video_id}",
                "originalurl": video_url,
                "creator": info.get("uploader", "Unknown"),
            }

            responses = upload(
                identifier=video_id,
                files=files_to_upload,
                metadata=ia_metadata,
                access_key=ia_creds.get("IA_ACCESS_KEY"),
                secret_key=ia_creds.get("IA_SECRET_KEY"),
            )

            if all(r.status_code == 200 for r in responses):
                logger.info(f"Archival successful for {video_id}")
                return True, info

            return False, None

        except Exception as e:
            logger.error(f"Archival failed for {video_id}: {str(e)}")
            return False, None
        finally:
            if work_dir.exists():
                shutil.rmtree(work_dir)
