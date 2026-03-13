import logging
import os
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def run_git_sync(config, new_count: int):
    """
    Synchronise the data directory with the remote Git repository.
    UK English spelling. Only performs actions if changes are detected.
    """
    if not config.get("git", "enabled", default=True) or new_count == 0:
        return True, "No new files to synchronise"

    data_dir = config.data_dir

    # Customise the commit message
    msg = config.get(
        "git",
        "commit_message_template",
        default="ia-archiver: {new_count} videos archived",
    ).format(new_count=new_count)

    try:
        # 1. Check for actual changes in the data directory
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=data_dir,
            capture_output=True,
            text=True,
            check=True,
        )

        if not status.stdout.strip():
            return True, "No actual changes detected in data directory"

        # 2. Stage changes
        subprocess.run(["git", "add", "."], cwd=data_dir, check=True)

        # 3. Create commit
        # Use env to ensure the identity is picked up if not set globally
        subprocess.run(["git", "commit", "-m", msg], cwd=data_dir, check=True)

        # 4. Push to remote (SSH)
        if config.get("git", "auto_push", default=True):
            branch = config.get("git", "branch", default="main")
            subprocess.run(["git", "push", "origin", branch], cwd=data_dir, check=True)
            return True, "Committed and Pushed successfully"

        return True, "Committed locally (auto_push disabled)"

    except subprocess.CalledProcessError as e:
        logger.error(
            f"Git synchronisation failed: {e.stderr if hasattr(e, 'stderr') else str(e)}"
        )
        return False, f"Git Error: {str(e)}"
