import argparse
import sys

from .main import run_job


def main():
    """
    Command Line Interface for the Internet Archive Archiver.
    UK English spelling and professionalised argument handling.
    """
    parser = argparse.ArgumentParser(
        description="DaGhE YouTube to Internet Archive Archiver"
    )

    parser.add_argument(
        "--config", required=True, help="Path to the module YAML configuration file"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the archival process without downloading or uploading",
    )

    parser.add_argument(
        "--verbose", action="store_true", help="Enable detailed debug logging"
    )

    args = parser.parse_args()

    # Launch the main job logic and capture the exit status
    try:
        exit_code = run_job(args.config, args.dry_run, args.verbose)
        sys.exit(exit_code)
    except Exception as e:
        print(f"CRITICAL: Unhandled exception in CLI entrypoint: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
