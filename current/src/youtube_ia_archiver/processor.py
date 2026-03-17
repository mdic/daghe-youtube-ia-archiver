import json
import logging
import os
import shutil
import socket
import time
from datetime import datetime
from pathlib import Path

import yt_dlp
from internetarchive import get_item, upload
from waybackpy import WaybackMachineCDXServerAPI, WaybackMachineSaveAPI

from .utils import sanitize_filename

logger = logging.getLogger(__name__)


class YdlLogger:
    """Helper to redirect yt-dlp logs to the DaGhE logging system."""

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
        """
        Initialise the processor with authenticated session logic.
        UK English spelling. Robust handling for metadata and media assets.
        """
        self.config = config
        self.ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": False,
            "writeinfojson": False,
            "noplaylist": True,
            "extract_flat": False,
            "logger": YdlLogger(),
        }

        # Merge Global YAML options (e.g. format, js_runtime)
        global_extras = self.config.global_ydl_opts
        if global_extras:
            self._apply_extra_opts(global_extras)

        # Apply YouTube authentication via shared cookies
        cookie_path = self.config.ydl_cookie_file
        if cookie_path and os.path.exists(cookie_path):
            self.ydl_opts["cookiefile"] = os.path.abspath(cookie_path)

    def _apply_extra_opts(self, extras: dict):
        """Standardise YAML types for the yt-dlp Python API."""
        for k, v in extras.items():
            if isinstance(v, str):
                if v.lower() == "true":
                    v = True
                elif v.lower() == "false":
                    v = False
            self.ydl_opts[k] = v

    def _get_ia_identifier(self, video_id: str) -> str:
        """Ensures the ID starts with an alphanumeric character to satisfy IA regex."""
        return f"yt-{video_id}"

    def get_playlist_video_ids(self) -> list:
        """Scans the playlist and saves metadata JSON to the data directory."""
        playlist_url = self.config.playlist_url
        data_dir = self.config.data_dir.absolute()

        scan_opts = self.ydl_opts.copy()
        scan_opts.update(
            {
                "extract_flat": "in_playlist",
                "skip_download": True,
                "ignore_no_formats_error": True,
                "writeinfojson": False,
                "outtmpl": {"default": str(data_dir / "playlist_%(id)s.%(ext)s")},
                "paths": {"home": str(data_dir)},
            }
        )

        logger.info(f"Initialising playlist scan: {playlist_url}")
        try:
            with yt_dlp.YoutubeDL(scan_opts) as ydl:
                result = ydl.extract_info(playlist_url, download=False)
                sanitized_result = ydl.sanitize_info(result)
                playlist_id = result.get("id", "unknown")
                playlist_json_path = data_dir / f"playlist_{playlist_id}.json"

                with open(playlist_json_path, "w", encoding="utf-8") as f:
                    json.dump(sanitized_result, f, indent=4, ensure_ascii=False)

                return [e["id"] for e in result.get("entries", []) if e.get("id")]
        except Exception as e:
            logger.error(f"Playlist synchronisation failed: {e}")
            return []

    def _archive_to_wayback(self, url: str) -> str:
        """UK English: Customised Wayback archival loop respecting YAML timeouts."""
        if not self.config.wayback_enabled:
            return ""

        ua = self.config.wayback_user_agent
        timeout = self.config.get_timeout_setting("wayback", "timeout_seconds", 60)
        polling = self.config.get_timeout_setting("wayback", "polling_seconds", 20)
        max_wait = self.config.get_timeout_setting("wayback", "max_wait_seconds", 450)

        socket.setdefaulttimeout(timeout)
        start_time = time.time()

        try:
            # Check for existing snapshots first
            cdx = WaybackMachineCDXServerAPI(url, ua)
            newest = cdx.newest()
            if newest and newest.archive_url:
                logger.info(f"Existing Wayback snapshot found: {newest.archive_url}")
                return newest.archive_url
        except Exception:
            pass

        # Retry loop for Save Page Now
        while (time.time() - start_time) < max_wait:
            try:
                save_api = WaybackMachineSaveAPI(url, ua)
                archived_url = save_api.save()
                if archived_url:
                    logger.info(f"New Wayback archival confirmed: {archived_url}")
                    return archived_url
            except Exception as e:
                logger.warning(f"Wayback retry in {polling}s due to: {e}")
            time.sleep(polling)

        return "N/A"

    def _wait_for_ia_availability(self, identifier: str):
        """Ensures the item is indexed by IA before concluding the task."""
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
            Path(os.getcwd()).absolute()
            / self.config.raw["ia_settings"]["description_template"]
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
                logger.error(f"Template formatting error: {e}")
        return metadata_context["description"]

    def process_video(
        self, video_id: str, dry_run: bool = False
    ) -> tuple[bool, dict | None, str]:
        """
        Executes the archival pipeline for a single video.
        Returns: (Success Boolean, Metadata Dict, Wayback URL)
        """
        work_dir = self.config.temp_work_dir.absolute() / video_id
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        ia_id = self._get_ia_identifier(video_id)

        if dry_run:
            logger.info(
                f"[Dry-run] Archival simulation for: {video_id} (IA ID: {ia_id})"
            )
            return True, None, "https://web.archive.org/dryrun"

        if work_dir.exists():
            shutil.rmtree(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            # 1. Download Media and Metadata assets
            local_opts = self.ydl_opts.copy()
            local_opts.update(
                {
                    "outtmpl": {"default": str(work_dir / "%(title)s.%(ext)s")},
                    "paths": {"home": str(work_dir)},
                }
            )

            logger.info(f"Initiating archival for {video_id}...")
            with yt_dlp.YoutubeDL(local_opts) as ydl:
                info = ydl.extract_info(video_url, download=True)
                title = info.get("title", "Unknown Title")

                # Inject the real IA ID for the inventory registry
                info["ia_identifier"] = ia_id
                sanitized_info = ydl.sanitize_info(info)

            # 2. Save Metadata JSON using a SANITISED filename
            # This prevents titles with slashes (e.g. 'w/') from breaking the filesystem
            safe_title = sanitize_filename(title)
            final_json_path = work_dir / f"{safe_title}.json"

            with open(final_json_path, "w", encoding="utf-8") as f:
                json.dump(sanitized_info, f, indent=4, ensure_ascii=False)

            logger.info(f"Metadata JSON preserved as: {safe_title}.json")

            # 3. Wayback Machine Archival
            wayback_url = self._archive_to_wayback(video_url)

            # 4. Internet Archive Credentials Loading
            ia_creds = {}
            if self.config.credentials_file.exists():
                for line in self.config.credentials_file.read_text().splitlines():
                    if "=" in line:
                        k, v = line.strip().split("=", 1)
                        ia_creds[k] = v.strip('"').strip("'")

            # 5. Upload to Internet Archive
            logger.info(f"Uploading assets to Internet Archive bucket: {ia_id}")
            files_to_upload = [str(f) for f in work_dir.iterdir() if f.is_file()]

            responses = upload(
                identifier=ia_id,
                files=files_to_upload,
                metadata={
                    "title": title,
                    "description": self._prepare_description(info),
                    "mediatype": "movies",
                    "collection": self.config.get("ia_settings", "collection"),
                    "external-identifier": f"youtube:{video_id}",
                    "originalurl": video_url,
                    "creator": info.get("uploader", "Unknown"),
                },
                access_key=ia_creds.get("IA_ACCESS_KEY"),
                secret_key=ia_creds.get("IA_SECRET_KEY"),
            )

            # 6. Final verification and polling
            if all(r.status_code == 200 for r in responses):
                self._wait_for_ia_availability(ia_id)
                return True, info, wayback_url

            return False, None, ""

        except Exception as e:
            logger.error(f"Pipeline failure for {video_id}: {e}")
            return False, None, ""
        finally:
            # Cleanup workspace immediately after the attempt
            if work_dir.exists():
                shutil.rmtree(work_dir)
