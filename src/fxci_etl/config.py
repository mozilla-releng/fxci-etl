import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dacite


@dataclass(frozen=True)
class PulseConfig:
    user: str
    password: str
    host: str = "pulse.mozilla.org"
    port: int = 5671
    queue: str = "fxci-etl"
    durable: bool = False
    auto_delete: bool = True


@dataclass(frozen=True)
class BigQueryConfig:
    project: str
    dataset: str
    tables: dict[str, str]


@dataclass(frozen=True)
class Config:
    pulse: PulseConfig
    bigquery: BigQueryConfig

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Config":
        return dacite.from_dict(data_class=cls, data=data)

    @classmethod
    def from_file(cls, path: str | Path) -> "Config":
        if isinstance(path, str):
            path = Path(path)

        with path.open("rb") as fh:
            return cls.from_dict(tomllib.load(fh))
