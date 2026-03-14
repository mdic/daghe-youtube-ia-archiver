import json
import logging
import os
import shutil
from pathlib import Path

import yt_dlp
from internetarchive import upload

logger = logging.getLogger(__name__)


class YdlLogger:
    """
    Helper to redirect yt-dlp logs to the DaGhE logging system.
    UK English spelling.
    """

    def debug(self, msg):
        # Filter out noisy debug messages but keep relevant info
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
        Initialise the processor with authenticated session and JS runtime.
        UK English: Robust handling for YouTube n-parameter challenges.
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
            "logger": YdlLogger(),
        }

        # 2. Merge Global YAML options (including js_runtime: "deno")
        global_extras = self.config.global_ydl_opts
        if global_extras:
            self._apply_extra_opts(global_extras)

        # 3. Apply Authentication
        cookie_path = self.config.ydl_cookie_file
        if cookie_path and os.path.exists(cookie_path):
            self.ydl_opts["cookiefile"] = os.path.abspath(cookie_path)
            logger.info(f"Authenticated session enabled via: {cookie_path}")

    def _apply_extra_opts(self, extras: dict):
        """Standardise YAML types for the yt-dlp Python API."""
        for k, v in extras.items():
            if isinstance(v, str):
                if v.lower() == "true":
                    v = True
                elif v.lower() == "false":
                    v = False
            self.ydl_opts[k] = v

    def get_playlist_video_ids(self) -> list:
        """Scan playlist for IDs using flat extraction mode."""
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
            logger.error(f"Playlist synchronisation failed: {e}")
            return []

    def _prepare_metadata(self, info: dict) -> dict:
        """Prepare metadata schema for Internet Archive upload."""
        title = info.get("title", "Unknown Title")
        v_id = info.get("id")

        # Resolve description template relative to module root
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
        """Archival pipeline: Metadata fetch -> Asset Download -> IA Upload -> Cleanup."""
        work_dir = self.config.temp_work_dir / video_id
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        if dry_run:
            logger.info(f"[Dry-run] Archival simulation for: {video_id}")
            return True

        if work_dir.exists():
            shutil.rmtree(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            # 1. Download assets with JS challenge solver enabled
            local_opts = self.ydl_opts.copy()
            local_opts["outtmpl"] = f"{work_dir}/%(title)s.%(ext)s"

            logger.info(f"Initiating archival process for {video_id}...")

            with yt_dlp.YoutubeDL(local_opts) as ydl:
                # This performs metadata extraction AND media download
                info = ydl.extract_info(video_url, download=True)
                title = info.get("title", "Unknown Title")

            # 2. Refine metadata naming (info.json -> Title.json)
            info_json = work_dir / f"{title}.info.json"
            if info_json.exists():
                shutil.move(str(info_json), str(work_dir / f"{title}.json"))

            # 3. Load Internet Archive Credentials
            ia_creds = {}
            if self.config.credentials_file.exists():
                for line in self.config.credentials_file.read_text().splitlines():
                    if "=" in line and not line.startswith("#"):
                        k, v = line.strip().split("=", 1)
                        ia_creds[k] = v.strip('"').strip("'")

            # 4. Upload payload to Internet Archive
            logger.info(f"Uploading assets to Internet Archive: {video_id}")
            files_to_upload = [str(f) for f in work_dir.iterdir() if f.is_file()]

            if not files_to_upload:
                logger.error(f"No files found to upload for {video_id}.")
                return False

            responses = upload(
                identifier=video_id,
                files=files_to_upload,
                metadata=self._prepare_metadata(info),
                access_key=ia_creds.get("IA_ACCESS_KEY"),
                secret_key=ia_creds.get("IA_SECRET_KEY"),
            )

            if all(r.status_code == 200 for r in responses):
                logger.info(f"Archival successful for {video_id}")
                return True

            logger.error(f"Internet Archive rejected assets for {video_id}")
            return False

        except Exception as e:
            logger.error(f"Archival failed for {video_id}: {str(e)}")
            return False
        finally:
            # 5. Purge temporary files to save VPS disk space
            if work_dir.exists():
                logger.info(f"Cleaning up temporary workspace: {work_dir}")
                shutil.rmtree(work_dir)
