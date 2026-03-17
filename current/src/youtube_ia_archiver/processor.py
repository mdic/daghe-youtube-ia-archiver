import json
import logging
import os
import shutil
import time
from datetime import datetime
from pathlib import Path

import yt_dlp
from internetarchive import get_item, upload

logger = logging.getLogger(__name__)


class YdlLogger:
    def debug(self, msg):
        if not msg.startswith("[debug] "):
            logger.debug(msg)

    def info(self, msg):
        logger.info(msg)

    def warning(self, msg):
        logger.warning(msg)

    def error(self, msg):
        logger.error(msg)


class ArchiveProcessor:
    def __init__(self, config):
        """Initialise with authenticated session logic and UK English logging."""
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
        """
        UK English: Scans the YouTube playlist.
        Saves metadata to data/playlist_<id>.json and prevents files in current/.
        """
        scan_opts = self.ydl_opts.copy()
        # CRITICAL: Disable automatic writeinfojson to prevent ghost files in current/
        scan_opts.update(
            {
                "extract_flat": "in_playlist",
                "skip_download": True,
                "ignore_no_formats_error": True,
                "writeinfojson": False,
            }
        )

        logger.info(f"Initialising playlist scan: {self.config.playlist_url}")
        try:
            with yt_dlp.YoutubeDL(scan_opts) as ydl:
                result = ydl.extract_info(self.config.playlist_url, download=False)

                playlist_id = result.get("id", "unknown")
                # Save the playlist JSON to the data folder with ID-based naming
                playlist_json_path = (
                    self.config.data_dir / f"playlist_{playlist_id}.json"
                )

                with open(playlist_json_path, "w", encoding="utf-8") as f:
                    json.dump(result, f, indent=4, ensure_ascii=False)

                logger.info(
                    f"Playlist metadata (ID: {playlist_id}) saved to: {playlist_json_path}"
                )

                return [e["id"] for e in result.get("entries", []) if e.get("id")]
        except Exception as e:
            logger.error(f"Playlist synchronisation failed: {e}")
            return []

    def _wait_for_ia_availability(self, identifier: str):
        """Polls Internet Archive to ensure the item is indexed after upload."""
        max_wait = self.config.get_timeout_setting("ia_upload", "max_wait_seconds", 900)
        polling = self.config.get_timeout_setting("ia_upload", "polling_seconds", 45)
        start_time = time.time()

        while (time.time() - start_time) < max_wait:
            if get_item(identifier).exists:
                logger.info(f"IA Item {identifier} is indexed and live.")
                return True
            time.sleep(polling)
        return False

    def _prepare_description(self, info: dict) -> str:
        """Constructs the final description using the external template."""
        template_path = (
            Path(os.getcwd()) / self.config.raw["ia_settings"]["description_template"]
        )
        metadata_context = {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "title": info.get("title", "N/A"),
            "description": info.get("description", "No description available."),
            "uploader": info.get("uploader", "Unknown Account"),
            "likes": info.get("like_count", "N/A"),
        }
        if template_path.exists():
            try:
                return template_path.read_text(encoding="utf-8").format(
                    **metadata_context
                )
            except Exception as e:
                logger.error(f"Placeholder substitution error: {e}")
        return metadata_context["description"]

    def process_video(
        self, video_id: str, dry_run: bool = False
    ) -> tuple[bool, dict | None]:
        """Executes the archival pipeline for a single video."""
        work_dir = self.config.temp_work_dir / video_id
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        if dry_run:
            logger.info(f"[Dry-run] Archival simulation for: {video_id}")
            return True, None

        if work_dir.exists():
            shutil.rmtree(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            # 1. Download media and original assets
            local_opts = self.ydl_opts.copy()
            local_opts["outtmpl"] = f"{work_dir}/%(title)s.%(ext)s"

            with yt_dlp.YoutubeDL(local_opts) as ydl:
                info = ydl.extract_info(video_url, download=True)
                title = info.get("title", "Unknown Title")

            # 2. Rename metadata JSON from info.json to [Title].json
            info_json = work_dir / f"{title}.info.json"
            if info_json.exists():
                shutil.move(str(info_json), str(work_dir / f"{title}.json"))

            # 3. Load IA Credentials from ia.env
            ia_creds = {}
            if self.config.credentials_file.exists():
                for line in self.config.credentials_file.read_text().splitlines():
                    if "=" in line:
                        k, v = line.strip().split("=", 1)
                        ia_creds[k] = v.strip('"').strip("'")

            # 4. Upload to Internet Archive
            logger.info(f"Uploading assets to Internet Archive: {video_id}")
            files_to_upload = [str(f) for f in work_dir.iterdir() if f.is_file()]

            ia_metadata = {
                "title": title,
                "description": self._prepare_description(info),
                "mediatype": "movies",
                "collection": self.config.get("ia_settings", "collection"),
                "external-identifier": f"youtube:{video_id}",
                "originalurl": f"https://www.youtube.com/watch?v={video_id}",
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
                self._wait_for_ia_availability(video_id)
                return True, info

            return False, None

        except Exception as e:
            logger.error(f"Pipeline failure for {video_id}: {e}")
            return False, None
        finally:
            # 5. Cleanup
            if work_dir.exists():
                shutil.rmtree(work_dir)
