import csv
import logging
import os
from logging.handlers import RotatingFileHandler

from .archive import ArchiveManager
from .config import load_config
from .git_ops import run_git_sync
from .notifier import send_notification
from .processor import ArchiveProcessor


def update_inventory(config, info: dict):
    """
    UK English: Updates the TSV registry using the normalised IA identifier.
    """
    if not config.inventory_enabled or not info:
        return

    file_path = config.inventory_file
    if not file_path:
        return

    file_exists = file_path.exists()
    header = ["youtube_id", "ia_identifier", "ia_url", "youtube_title"]

    # We retrieve the sanitised ID injected in processor.py
    video_id = info.get("id")
    ia_id = info.get("ia_identifier", f"yt-{video_id}")

    row = {
        "youtube_id": video_id,
        "ia_identifier": ia_id,
        "ia_url": f"https://archive.org/details/{ia_id}",
        "youtube_title": info.get("title", "Unknown Title"),
    }

    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header, delimiter="\t")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
        logging.info(f"Inventory TSV synchronised for: {ia_id}")
    except Exception as e:
        logger.error(f"TSV update failed: {e}")


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
        fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=3)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except Exception as e:
        print(f"Log error: {e}")

    archive = ArchiveManager(config.archive_file)
    processor = ArchiveProcessor(config)

    processed = 0
    failed = []

    ids = processor.get_playlist_video_ids()
    to_do = [i for i in ids if not archive.is_processed(i)]

    logger.info(f"Archival batch initiated: {len(to_do)} new items.")

    for vid in to_do:
        success, info = processor.process_video(vid, dry_run=dry_run)
        if success and not dry_run:
            archive.add(vid)
            update_inventory(config, info)
            processed += 1
        elif not success:
            failed.append(vid)

    git_success, git_msg = (
        (True, "Skipped") if dry_run else run_git_sync(config, processed)
    )
    status = (
        "success"
        if not failed and git_success
        else ("partial" if processed > 0 else "failure")
    )

    summary = f"Job: {config.get('job_name')}\nArchived: {processed}\nStatus: {status.upper()}"
    if not dry_run:
        send_notification(
            config,
            config.get("telegram", f"level_on_{status}", default="info"),
            summary,
        )

    print(summary)
    return 0 if status == "success" else 2
