import csv
import logging
import os
from logging.handlers import RotatingFileHandler

from .archive import ArchiveManager
from .config import load_config
from .git_ops import run_git_sync
from .notifier import send_notification
from .processor import ArchiveProcessor
from .utils import get_dir_size_human


def update_inventory(config, info: dict):
    """
    Appends a new row to the TSV inventory file.
    UK English: Standardises archival records with customisable columns.
    """
    if not config.inventory_enabled or not info:
        return

    inventory_file = config.inventory_file
    file_exists = inventory_file.exists()
    columns = config.inventory_columns

    # Custom mapping for calculated fields
    custom_fields = {"ia_url": f"https://archive.org/details/{info.get('id')}"}

    # Extract row data based on requested columns
    row = {}
    for col in columns:
        # Check custom fields first, then yt-dlp info dict
        row[col] = custom_fields.get(col, info.get(col, "N/A"))

    try:
        with open(inventory_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns, delimiter="\t")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
        logging.info(f"Inventory updated for video: {info.get('id')}")
    except Exception as e:
        logging.error(f"Failed to update inventory TSV: {e}")


def run_job(config_path: str, dry_run: bool, verbose: bool):
    config = load_config(config_path)

    log_level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

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
        print(f"Warning: File logging failed: {e}")

    archive = ArchiveManager(config.archive_file)
    processor = ArchiveProcessor(config)

    processed = 0
    failed = []

    ids = processor.get_playlist_video_ids()
    to_do = [i for i in ids if not archive.is_processed(i)]

    logger.info(f"Processing cycle started. {len(to_do)} new videos detected.")

    for vid in to_do:
        # process_video now returns (success, info_dict)
        success, info_dict = processor.process_video(vid, dry_run=dry_run)
        if success and not dry_run:
            archive.add(vid)
            # Add to the custom TSV inventory
            update_inventory(config, info_dict)
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

    summary = (
        f"Job: {config.get('job_name')}\n"
        f"Videos Scanned: {len(ids)}\n"
        f"New Archived: {processed}\n"
        f"Failed: {len(failed)}\n"
        f"Git Sync: {git_msg}\n"
        f"Status: {status.upper()}"
    )

    if not dry_run:
        send_notification(
            config,
            config.get("telegram", f"level_on_{status}", default="info"),
            summary,
        )

    print(summary)
    return 0 if status == "success" else 2
