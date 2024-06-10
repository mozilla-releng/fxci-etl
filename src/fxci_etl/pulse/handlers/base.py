from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from kombu import Message

from fxci_etl.config import Config
from fxci_etl.util.python_path import import_sibling_modules


@dataclass
class Event:
    data: dict[str, Any]
    message: Message


class PulseHandler(ABC):
    name = ""

    def __init__(self, config: Config, buffered=True):
        self.config = config
        self.buffered = buffered
        self._buffer: list[Event] = []

    def __call__(self, data: dict[str, Any], message: Message) -> None:
        message.ack()
        event = Event(data, message)
        if self.buffered:
            self._buffer.append(event)
        else:
            self.process_events([event])

    def process_buffer(self):
        self.process_events(self._buffer)
        self._buffer = []

    @abstractmethod
    def process_events(self, events: list[Event]) -> None: ...


handlers: dict[str, type[PulseHandler]] = {}


def register() -> Callable[[type[PulseHandler]], None]:
    def inner(cls: type[PulseHandler]) -> None:
        handlers[cls.name] = cls

    return inner


import_sibling_modules()
