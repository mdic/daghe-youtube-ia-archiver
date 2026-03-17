import logging
import os
import re
import subprocess
from logging.handlers import RotatingFileHandler
from pathlib import Path


def sanitize_filename(filename: str) -> str:
    if not filename:
        return "unknown_title"
    # Forza a stringa, rimuove caratteri illegali
    filename = str(filename)
    filename = re.sub(r"(?u)[^-\w. ]", "", filename)
    filename = " ".join(filename.split())
    return filename[:200] if filename else "unknown_title"


def setup_logging(config, verbose: bool):
    """
    Initialise a robust logging system with console and rotating file handlers.
    UK English: Standardises logs within the authorised DaGhE logs directory.
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(log_level)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Handler for terminal output
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Handler for persistent file logging
    # BASE_DIR is resolved from config via os.path.expandvars
    base_dir = os.path.expandvars("${BASE_DIR}")
    log_file = os.path.join(base_dir, "logs", "daghe-youtube-ia-archiver.log")

    try:
        # Create a rotating log: 10MB per file, keeping 3 historical backups
        fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=3)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logging.info(f"Logging initialised. Target file: {log_file}")
    except Exception as e:
        print(f"Warning: Failed to initialise file logging at {log_file}: {e}")

    return logger


def get_dir_size_human(directory: Path) -> str:
    """
    Calculates the human-readable size of a directory.
    UK English: Standardises units for archival reporting.
    """
    total_size = 0
    if not directory.exists():
        return "0 B"
    for fp in directory.glob("**/*"):
        if fp.is_file():
            total_size += fp.stat().st_size

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if total_size < 1024.0:
            return f"{total_size:.2f} {unit}"
        total_size /= 1024.0
    return f"{total_size:.2f} PB"
