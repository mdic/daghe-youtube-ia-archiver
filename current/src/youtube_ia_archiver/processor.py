import logging
import os
import shutil
from pathlib import Path

import yt_dlp
from internetarchive import upload

logger = logging.getLogger(__name__)


class YdlLogger:
    """Helper to redirect yt-dlp logs to DaGhE logging system."""

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
        """
        Initialise the processor.
        UK English spelling. Robust type handling for yt-dlp options.
        """
        self.config = config

        # 1. Base default options
        self.ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": False,
            "writeinfojson": True,
            "noplaylist": True,
            "extract_flat": False,
            "logger": YdlLogger(),  # Attach our custom logger
        }

        # 2. Merge Global YAML options with type enforcement
        global_extras = self.config.global_ydl_opts
        if global_extras:
            self._apply_extra_opts(global_extras)

        # 3. Apply Authentication
        cookie_path = self.config.ydl_cookie_file
        if cookie_path and os.path.exists(cookie_path):
            self.ydl_opts["cookiefile"] = os.path.abspath(cookie_path)
            logger.info(f"Using shared cookies from: {cookie_path}")

    def _apply_extra_opts(self, extras: dict):
        """Ensure YAML types are correctly converted for Python API."""
        for k, v in extras.items():
            # Convert string "true"/"false" to actual Booleans
            if isinstance(v, str):
                if v.lower() == "true":
                    v = True
                elif v.lower() == "false":
                    v = False
            self.ydl_opts[k] = v

    def get_playlist_video_ids(self) -> list:
        """Scan playlist for IDs using flat extraction."""
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

    def _prepare_metadata(self, info: dict) -> dict:
        """Prepare metadata schema for Internet Archive upload."""
        title = info.get("title", "Unknown Title")
        v_id = info.get("id")

        # Resolve description template
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
            "external-identifier": f"youtube:{v_id}",
            "originalurl": f"https://www.youtube.com/watch?v={v_id}",
            "creator": info.get("uploader", "Unknown"),
        }

    def process_video(self, video_id: str, dry_run: bool = False) -> bool:
        """Pipeline: Download -> Metadata Rename -> IA Upload -> Cleanup."""
        work_dir = self.config.temp_work_dir / video_id
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        if dry_run:
            logger.info(f"[Dry-run] Simulation for video: {video_id}")
            return True

        if work_dir.exists():
            shutil.rmtree(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            # 1. Asset Download
            local_opts = self.ydl_opts.copy()
            local_opts["outtmpl"] = f"{work_dir}/%(title)s.%(ext)s"

            logger.info(f"Starting archival for {video_id}...")

            with yt_dlp.YoutubeDL(local_opts) as ydl:
                # This call will now use our YdlLogger to show details in CLI
                info = ydl.extract_info(video_url, download=True)
                title = info.get("title", "Unknown Title")

            # 2. Rename metadata JSON
            info_json = work_dir / f"{title}.info.json"
            if info_json.exists():
                shutil.move(str(info_json), str(work_dir / f"{title}.json"))

            # 3. Load IA Credentials
            ia_creds = {}
            creds_file = self.config.credentials_file
            if creds_file.exists():
                for line in creds_file.read_text().splitlines():
                    if "=" in line and not line.startswith("#"):
                        k, v = line.strip().split("=", 1)
                        ia_creds[k] = v.strip('"').strip("'")

            # 4. Upload to IA
            logger.info(f"Uploading files to Internet Archive: {video_id}")
            files_to_upload = [str(f) for f in work_dir.iterdir() if f.is_file()]

            responses = upload(
                identifier=video_id,
                files=files_to_upload,
                metadata=self._prepare_metadata(info),
                access_key=ia_creds.get("IA_ACCESS_KEY"),
                secret_key=ia_creds.get("IA_SECRET_KEY"),
            )

            if all(r.status_code == 200 for r in responses):
                logger.info(f"Archival successful: {video_id}")
                return True

            return False

        except Exception as e:
            logger.error(f"Archival failed for {video_id}: {str(e)}")
            return False
        finally:
            if work_dir.exists():
                shutil.rmtree(work_dir)
