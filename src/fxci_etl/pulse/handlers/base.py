from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from fxci_etl.config import Config
from fxci_etl.util.python_path import import_sibling_modules


class PulseHandler(ABC):
    name = ""

    def __init__(self, config: Config):
        self.config = config

    @abstractmethod
    def __call__(self, data: dict[str, Any], message: str) -> None: ...


handlers: dict[str, type[PulseHandler]] = {}


def register() -> Callable[[type[PulseHandler]], None]:
    def inner(cls: type[PulseHandler]) -> None:
        handlers[cls.name] = cls

    return inner


import_sibling_modules()
