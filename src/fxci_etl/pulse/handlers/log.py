from pprint import pprint
from typing import Any

from fxci_etl.pulse.handlers import PulseHandler, register


@register()
class LogHandler(PulseHandler):
    def __call__(self, data: dict[str, Any], message: str) -> None:
        pprint(data, indent=2)
        pprint(message, indent=2)
