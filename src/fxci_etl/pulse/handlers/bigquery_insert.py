from pprint import pprint
from typing import Any

from fxci_etl.pulse.handlers import register


@register()
def insert_record(data: dict[str, Any], message: str) -> None:
    print("=====")
    pprint(data, indent=2)
    pprint(message, indent=2)
