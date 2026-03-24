import csv
import logging
import os
import time
from logging.handlers import RotatingFileHandler

from .archive import ArchiveManager
from .config import load_config
from .git_ops import run_git_sync
from .notifier import send_notification
from .processor import ArchiveProcessor


def update_inventory(config, info: dict, wayback_url: str):
    """UK English: Records metadata and archival URLs to the TSV registry."""
    if not config.inventory_enabled or not info:
        return

    file_path = config.inventory_file
    if not file_path:
        return

    file_exists = file_path.exists()
    header = ["youtube_id", "ia_identifier", "wayback_url", "youtube_title"]

    video_id = info.get("id")
    ia_id = info.get("ia_identifier", f"yt-{video_id}")

    row = {
        "youtube_id": video_id,
        "ia_identifier": ia_id,
        "wayback_url": wayback_url,
        "youtube_title": info.get("title", "Unknown Title"),
    }

    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header, delimiter="\t")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
        logging.info(f"Inventory synchronised: {video_id}")
    except Exception as e:
        logging.error(f"TSV update failed: {e}")


def run_job(config_path: str, dry_run: bool, verbose: bool):
    config = load_config(config_path)

    log_level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    log_file = os.path.join(
        os.path.expandvars("${BASE_DIR}"), "logs", "daghe-youtube-ia-archiver.log"
    )
    try:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=3)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except Exception:
        pass

    archive = ArchiveManager(config.archive_file)
    processor = ArchiveProcessor(config)

    processed = 0
    failed = []

    ids = processor.get_playlist_video_ids()
    to_do = [i for i in ids if not archive.is_processed(i)]

    logger.info(f"Archival sequence initiated: {len(to_do)} new items.")

    for i, vid in enumerate(to_do):
        success, info, wb_url = processor.process_video(vid, dry_run=dry_run)

        if success and not dry_run:
            archive.add(vid)
            update_inventory(config, info, wb_url)
            processed += 1
        elif not success:
            failed.append(vid)

        # Enforce inter-item pacing (UK English spelling)
        if i < len(to_do) - 1:  # Skip delay after the final item
            delay = config.ia_inter_item_delay
            logger.info(
                f"Pacing archival sequence: Initialising {delay}s delay before next item."
            )
            time.sleep(delay)

    git_success, git_msg = (
        (True, "Skipped") if dry_run else run_git_sync(config, processed)
    )

    # Calculate Job Status
    if not failed and git_success:
        status = "success"
    elif processed > 0:
        status = "partial"
    else:
        status = "failure"

    # Notification Mapping (Coherent and backward-compatible)
    def dispatch_notification(current_status):
        if current_status == "success":
            if not config.get("telegram", "notify_on_success", default=True):
                return
            level = config.get("telegram", "level_on_success", default="info")
        elif current_status == "partial":
            level = config.get("telegram", "level_on_partial", default="warning")
        else:
            level = config.get("telegram", "level_on_failure", default="error")

        job_summary = f"Job: {config.job_name}\nArchived: {processed}\nStatus: {current_status.upper()}"
        send_notification(config, level, job_summary)

    if not dry_run:
        dispatch_notification(status)

    print(f"Job: {config.job_name}\nArchived: {processed}\nStatus: {status.upper()}")
    return 0 if status == "success" else 2
