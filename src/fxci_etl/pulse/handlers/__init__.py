from collections.abc import Callable
from typing import Any

from fxci_etl.util.python_path import import_sibling_modules

PulseHandler = Callable[[dict[str, Any], str], None]
handlers: list[PulseHandler] = []


def register() -> Callable[[PulseHandler], None]:
    def inner(func: PulseHandler) -> None:
        handlers.append(func)

    return inner


import_sibling_modules()
