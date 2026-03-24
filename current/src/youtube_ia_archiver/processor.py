import fcntl
import json
import logging
import os
import shutil
import socket
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import yt_dlp
from internetarchive import get_item, get_session, upload
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


def _get_ia_wait_time(response, default_backoff):
    """UK English: Extracts wait duration from Retry-After or fallback."""
    header = response.headers.get("Retry-After")
    if not header:
        return default_backoff
    try:
        if header.isdigit():
            return int(header)
        retry_date = parsedate_to_datetime(header)
        delta = (retry_date - datetime.now(timezone.utc)).total_seconds()
        return max(int(delta), 1)
    except Exception:
        return default_backoff


def _check_ia_throttling(responses):
    """UK English: Detects IA rate-limiting signals in response list."""
    if not responses:
        return False, None
    for r in responses:
        # Safe handling for potential None/empty response body
        body_lower = (r.text or "").lower()
        # Explicit detection strings from official IA guidance
        is_limit_msg = any(
            m in body_lower for m in ["slowdown", "reduce your request rate", "spam"]
        )
        if r.status_code in [429, 503] or is_limit_msg:
            return True, r
    return False, None


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

        # Merge Global YAML options
        global_extras = self.config.global_ydl_opts
        if global_extras:
            self._apply_extra_opts(global_extras)

        # Apply YouTube authentication
        cookie_path = self.config.ydl_cookie_file
        if cookie_path and os.path.exists(cookie_path):
            self.ydl_opts["cookiefile"] = os.path.abspath(cookie_path)

        # Initialise Internet Archive Identity via the supported public API
        session = get_session()
        ua_suffix = self.config.ia_user_agent_suffix
        current_ua = session.headers.get("User-Agent", "")
        if ua_suffix not in current_ua:
            session.headers["User-Agent"] = f"{current_ua} {ua_suffix}".strip()

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
        """Ensures the ID starts with an alphanumeric character."""
        return f"yt-{video_id}"

    def _load_ia_credentials(self) -> dict:
        ia_creds = {}
        if self.config.credentials_file.exists():
            for line in self.config.credentials_file.read_text().splitlines():
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    ia_creds[k] = v.strip('"').strip("'")
        return ia_creds

    def get_playlist_video_ids(self) -> list:
        """Scans the playlist and saves metadata JSON."""
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
        """Customised Wayback archival loop respecting YAML timeouts."""
        if not self.config.wayback_enabled:
            return ""

        ua = self.config.wayback_user_agent
        timeout = self.config.get_timeout_setting("wayback", "timeout_seconds", 60)
        polling = self.config.get_timeout_setting("wayback", "polling_seconds", 20)
        max_wait = self.config.get_timeout_setting("wayback", "max_wait_seconds", 450)

        socket.setdefaulttimeout(timeout)
        start_time = time.time()

        try:
            cdx = WaybackMachineCDXServerAPI(url, ua)
            newest = cdx.newest()
            if newest and newest.archive_url:
                logger.info(f"Existing Wayback snapshot found: {newest.archive_url}")
                return newest.archive_url
        except Exception:
            pass

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
        """Ensures the item is indexed by IA before concluding."""
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
        """Constructs final description using external template."""
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
        Executes archival pipeline for a single video.
        Returns: (Success, Metadata Dict, Wayback URL)
        """
        work_dir = self.config.temp_work_dir.absolute() / video_id
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        ia_id = self._get_ia_identifier(video_id)

        if dry_run:
            logger.info(f"[Dry-run] Archival simulation for: {video_id}")
            return True, None, "https://web.archive.org/dryrun"

        if work_dir.exists():
            shutil.rmtree(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            # 1. Download Media and Metadata
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
                info["ia_identifier"] = ia_id
                sanitized_info = ydl.sanitize_info(info)

            safe_title = sanitize_filename(title)
            final_json_path = work_dir / f"{safe_title}.json"
            with open(final_json_path, "w", encoding="utf-8") as f:
                json.dump(sanitized_info, f, indent=4, ensure_ascii=False)

            # 2. Wayback Machine
            wayback_url = self._archive_to_wayback(video_url)

            # 3. Internet Archive Upload Preparation
            ia_creds = self._load_ia_credentials()
            files_to_upload = [str(f) for f in work_dir.iterdir() if f.is_file()]
            lock_path = Path(os.path.expandvars("${BASE_DIR}/data/ia_global.lock"))
            lock_path.parent.mkdir(parents=True, exist_ok=True)

            metadata_dict = {
                "title": title,
                "description": self._prepare_description(info),
                "mediatype": "movies",
                "collection": self.config.get("ia_settings", "collection"),
                "external-identifier": f"youtube:{video_id}",
                "originalurl": video_url,
                "creator": info.get("uploader", "Unknown"),
            }

            def perform_ia_upload():
                """Scoped upload execution with configured timeout."""
                timeout = self.config.get_timeout_setting(
                    "ia_upload", "timeout_seconds", 600
                )
                logger.info(f"IA upload started for: {ia_id}")
                return upload(
                    identifier=ia_id,
                    files=files_to_upload,
                    metadata=metadata_dict,
                    access_key=ia_creds.get("IA_ACCESS_KEY"),
                    secret_key=ia_creds.get("IA_SECRET_KEY"),
                    request_kwargs={"timeout": timeout},
                )

            # 4. Initial Attempt with Atomic Global Lock (DaGhE-wide IA concurrency protection)
            with open(lock_path, "a") as lock_file:
                fcntl.flock(lock_file, fcntl.LOCK_EX)
                responses = perform_ia_upload()

            # 5. Throttling and Single Retry Logic
            is_throttled, throttled_res = _check_ia_throttling(responses)
            if is_throttled:
                wait_time = _get_ia_wait_time(
                    throttled_res, self.config.ia_rate_limit_backoff
                )
                logger.warning(
                    f"IA throttling detected (Status: {throttled_res.status_code}). Backoff: {wait_time}s."
                )
                time.sleep(wait_time)

                logger.info("Retry attempt started.")
                with open(lock_path, "a") as lock_file:
                    fcntl.flock(lock_file, fcntl.LOCK_EX)
                    responses = perform_ia_upload()

                is_throttled, throttled_res = _check_ia_throttling(responses)
                if is_throttled:
                    logger.error(
                        "Retry failed due to continued throttling -> aborting item."
                    )
                    return False, None, ""

            # 6. Final Response Validation and Polling
            if responses and all(r.status_code == 200 for r in responses):
                logger.info("Upload successful.")
                self._wait_for_ia_availability(ia_id)
                return True, info, wayback_url

            logger.error(
                f"Archival failed for {video_id}: Terminal IA error or empty response."
            )
            return False, None, ""

        except Exception as e:
            logger.error(f"Pipeline failure for {video_id}: {e}")
            return False, None, ""
        finally:
            if work_dir.exists():
                shutil.rmtree(work_dir)
