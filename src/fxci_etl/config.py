import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import dacite


@dataclass(frozen=True)
class EtlConfig:
    handlers: list[str] | None = None

    def __post_init__(self):
        from fxci_etl.pulse.handlers import handlers

        if self.handlers:
            for handler in self.handlers:
                assert handler in handlers


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
    etl: EtlConfig = field(default_factory=EtlConfig)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Config":
        return dacite.from_dict(data_class=cls, data=data)

    @classmethod
    def from_file(cls, path: str | Path) -> "Config":
        if isinstance(path, str):
            path = Path(path)

        with path.open("rb") as fh:
            return cls.from_dict(tomllib.load(fh))
