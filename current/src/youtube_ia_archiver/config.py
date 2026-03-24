import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class JobConfig:
    raw: dict

    def _expand_path(self, path_str: str) -> Path:
        """Expand environment variables like ${BASE_DIR}."""
        if not path_str:
            return None
        return Path(os.path.expandvars(str(path_str)))

    @property
    def job_name(self) -> str:
        return self.raw.get("job_name", "unknown-job")

    @property
    def playlist_url(self) -> str:
        return self.raw.get("playlist_url", "")

    @property
    def data_dir(self) -> Path:
        return self._expand_path(self.raw.get("paths", {}).get("data_dir"))

    @property
    def archive_file(self) -> Path:
        return self._expand_path(self.raw.get("paths", {}).get("archive_file"))

    @property
    def inventory_file(self) -> Path:
        return self._expand_path(self.raw.get("paths", {}).get("inventory_tsv"))

    @property
    def temp_work_dir(self) -> Path:
        return self._expand_path(self.raw.get("paths", {}).get("temp_work_dir"))

    @property
    def telegram_helper(self) -> str:
        path = self.raw.get("paths", {}).get("telegram_helper")
        return os.path.expandvars(path) if path else ""

    @property
    def ydl_cookie_file(self) -> str | None:
        """Returns the absolute path to the shared cookie file."""
        path = self.raw.get("yt_dlp", {}).get("cookie_file")
        return os.path.expandvars(path) if path else None

    @property
    def global_ydl_opts(self) -> dict:
        """Returns extra yt-dlp options from the YAML."""
        return self.raw.get("yt_dlp", {}).get("extra_ydl_opts", {})

    @property
    def credentials_file(self) -> Path:
        return self._expand_path(
            self.raw.get("ia_settings", {}).get("credentials_file")
        )

    @property
    def inventory_enabled(self) -> bool:
        return self.raw.get("inventory", {}).get("enabled", False)

    @property
    def wayback_enabled(self) -> bool:
        """UK English: Checks if Wayback Machine archival is enabled."""
        return self.raw.get("wayback", {}).get("enabled", False)

    @property
    def wayback_user_agent(self) -> str:
        return self.raw.get("wayback", {}).get("user_agent", "DaGhE Bot")

    @property
    def timeouts(self) -> dict:
        return self.raw.get("timeouts", {})

    def get_timeout_setting(self, platform: str, key: str, default: int) -> int:
        """Retrieves specific timeout or polling intervals from config."""
        return self.timeouts.get(platform, {}).get(key, default)

    @property
    def ia_inter_item_delay(self) -> int:
        """UK English: Base delay between separate video archival cycles."""
        return self.get_timeout_setting("ia_upload", "inter_item_delay_seconds", 30)

    @property
    def ia_inter_item_increment(self) -> int:
        """UK English: Pacing increment added per item processed in a run."""
        return self.get_timeout_setting("ia_upload", "inter_item_increment_seconds", 10)

    @property
    def ia_inter_item_max_delay(self) -> int:
        """UK English: Maximum ceiling for adaptive inter-item pacing."""
        return self.get_timeout_setting(
            "ia_upload", "inter_item_max_delay_seconds", 300
        )

    @property
    def ia_rate_limit_backoff(self) -> int:
        """UK English: Fallback delay if IA throttles or is overloaded."""
        return self.get_timeout_setting("ia_upload", "rate_limit_backoff_seconds", 300)

    @property
    def ia_user_agent_suffix(self) -> str:
        """UK English: Identity string for IA automated tool identification."""
        return self.raw.get("ia_settings", {}).get(
            "user_agent_suffix", "DaGhE/2.x (module=daghe-youtube-ia-archiver)"
        )

    def get(self, *keys, default=None):
        """Deep get utility for nested dictionaries."""
        data = self.raw
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key)
            else:
                return default
        return data if data is not None else default


def load_config(path: str) -> JobConfig:
    with open(path, "r") as f:
        return JobConfig(yaml.safe_load(f))
