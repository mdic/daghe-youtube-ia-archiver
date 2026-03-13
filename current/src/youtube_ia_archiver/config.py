import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class JobConfig:
    raw: dict

    def _expand_path(self, path_str: str) -> Path:
        return Path(os.path.expandvars(str(path_str)))

    @property
    def playlist_url(self) -> str:
        return self.raw.get("playlist_url", "")

    @property
    def data_dir(self) -> Path:
        return self._expand_path(self.raw["paths"]["data_dir"])

    @property
    def archive_file(self) -> Path:
        return self._expand_path(self.raw["paths"]["archive_file"])

    @property
    def temp_work_dir(self) -> Path:
        return self._expand_path(self.raw["paths"]["temp_work_dir"])

    @property
    def credentials_file(self) -> Path:
        return self._expand_path(self.raw["ia_settings"]["credentials_file"])

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
