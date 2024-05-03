import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dacite


@dataclass
class PulseConfig:
    user: str
    password: str
    host: str = "pulse.mozilla.org"
    port: int = 5671
    queue: str = "fxci-etl"
    durable: bool = False
    auto_delete: bool = True


@dataclass
class Config:
    pulse: PulseConfig

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Config":
        return dacite.from_dict(data_class=cls, data=data)

    @classmethod
    def from_file(cls, path: str | Path) -> "Config":
        if isinstance(path, str):
            path = Path(path)

        with path.open("rb") as fh:
            return cls.from_dict(tomllib.load(fh))