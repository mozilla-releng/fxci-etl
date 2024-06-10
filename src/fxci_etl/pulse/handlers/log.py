from pprint import pprint

from fxci_etl.pulse.handlers.base import PulseHandler, register


@register()
class LogHandler(PulseHandler):
    name = "log"

    def process_events(self, events) -> None:
        for event in events:
            pprint(event, indent=2)
