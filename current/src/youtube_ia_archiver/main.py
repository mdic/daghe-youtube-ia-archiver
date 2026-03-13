import logging
import os
from logging.handlers import RotatingFileHandler

from .archive import ArchiveManager
from .config import load_config
from .git_ops import run_git_sync
from .notifier import send_notification
from .processor import ArchiveProcessor
from .utils import get_dir_size_human


def run_job(config_path: str, dry_run: bool, verbose: bool):
    """
    Orchestrates the archival loop.
    UK English: Initialises logging and aggregates results for reporting.
    """
    config = load_config(config_path)

    # 1. Setup Logging
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

    # 2. Pipeline Execution
    ids = processor.get_playlist_video_ids()
    to_do = [i for i in ids if not archive.is_processed(i)]

    logger.info(f"Processing cycle started. {len(to_do)} new videos detected.")

    for vid in to_do:
        if processor.process_video(vid, dry_run=dry_run):
            if not dry_run:
                archive.add(vid)
            processed += 1
        else:
            failed.append(vid)

    # 3. Synchronisation & Final Report
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
