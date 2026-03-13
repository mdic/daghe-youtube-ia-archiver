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
    def playlist_url(self) -> str:
        return self.raw.get("playlist_url", "")

    @property
    def data_dir(self) -> Path:
        return self._expand_path(self.raw.get("paths", {}).get("data_dir"))

    @property
    def archive_file(self) -> Path:
        return self._expand_path(self.raw.get("paths", {}).get("archive_file"))

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

    def get(self, *keys, default=None):
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
