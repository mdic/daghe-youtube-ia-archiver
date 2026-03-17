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
    if not config.get("git", "enabled", default=True):
        return True, "Git synchronisation disabled in config"

    # We skip commit if no new items were processed to avoid empty commits
    if new_count == 0:
        return True, "No new files to synchronise"

    data_dir = config.data_dir
    branch = config.get("git", "branch", default="main")
    msg_template = config.get(
        "git", "commit_message_template", default="ia-archiver: {new_count} new entries"
    )
    commit_msg = msg_template.format(new_count=new_count)

    try:
        # 1. Check for actual changes (archived_videos.txt or archived_inventory.tsv)
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=data_dir,
            capture_output=True,
            text=True,
            check=True,
        )

        if not status.stdout.strip():
            return True, "No actual changes detected in data directory"

        # 2. Stage all changes within the data folder
        subprocess.run(["git", "add", "."], cwd=data_dir, check=True)

        # 3. Create a single atomic commit
        subprocess.run(["git", "commit", "-m", commit_msg], cwd=data_dir, check=True)

        # 4. Push to remote using the SSH key configured in the daghe session
        if config.get("git", "auto_push", default=True):
            logger.info(f"Pushing changes to remote branch: {branch}")
            subprocess.run(["git", "push", "origin", branch], cwd=data_dir, check=True)
            return True, "Committed and Pushed successfully"

        return True, "Committed locally (auto_push disabled)"

    except subprocess.CalledProcessError as e:
        # Provide detailed error info for debugging via 'daghe logs'
        err_detail = e.stderr if hasattr(e, "stderr") else str(e)
        logger.error(f"Git operation failed: {err_detail}")
        return False, f"Git Error: {err_detail}"
