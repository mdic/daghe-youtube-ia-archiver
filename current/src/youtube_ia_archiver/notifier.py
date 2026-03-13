import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def send_notification(config, level: str, message: str):
    """
    Initialise the notification sequence via the shared DaGhE Telegram helper.
    UK English spelling. Ensures the helper script is accessible and executable.
    """
    if not config.get("telegram", "enabled", default=True):
        return

    # Path to the shared bash script, resolved via environment variables
    helper = config.telegram_helper

    if not helper:
        logger.warning("Telegram helper path not defined in configuration.")
        return

    if not os.path.exists(helper):
        logger.error(f"Telegram helper not found at: {helper}")
        return

    if not os.access(helper, os.X_OK):
        logger.error(f"Permission denied: {helper} is not executable. Run 'chmod +x'.")
        return

    try:
        # Standardised invocation: helper <level> <message>
        subprocess.run([helper, level.lower(), message], check=True)
        logger.info(f"Telegram notification dispatched (Level: {level.upper()}).")
    except subprocess.CalledProcessError as e:
        logger.error(f"Telegram helper returned non-zero exit status: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during notification dispatch: {e}")
