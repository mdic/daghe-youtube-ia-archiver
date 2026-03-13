import logging
import os

from .archive import ArchiveManager
from .config import load_config
from .processor import ArchiveProcessor
from .utils import send_telegram_summary, setup_logging


def run_job(config_path: str, dry_run: bool, verbose: bool):
    config = load_config(config_path)
    logger = setup_logging(config, verbose)

    archive = ArchiveManager(config.archive_file)
    processor = ArchiveProcessor(config)

    processed_count = 0
    errors = []

    # 1. Initialise scan
    all_ids = processor.get_playlist_video_ids()
    new_ids = [vid for vid in all_ids if not archive.is_processed(vid)]

    logger.info(f"Found {len(all_ids)} videos, {len(new_ids)} are new.")

    # 2. Process loop
    for vid in new_ids:
        try:
            if processor.process_video(vid, dry_run=dry_run):
                if not dry_run:
                    archive.add(vid)
                processed_count += 1
        except Exception as e:
            logger.error(f"Fatal error processing {vid}: {e}")
            errors.append(vid)

    # 3. Finalise
    summary = (
        f"Job: {config.get('job_name')}\n"
        f"New Archived: {processed_count}\n"
        f"Failed: {len(errors)}\n"
        f"Status: {'SUCCESS' if not errors else 'PARTIAL'}"
    )

    if not dry_run:
        send_telegram_summary(config, summary)

    print(summary)
    return 0 if not errors else 2
