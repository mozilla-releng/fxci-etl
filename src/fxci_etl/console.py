import asyncio
import sys
from dataclasses import asdict
from pathlib import Path

import appdirs
from cleo.application import Application
from cleo.commands.command import Command
from cleo.helpers import argument, option

from fxci_etl.config import Config
from fxci_etl.pulse.listen import listen

APP_NAME = "fxci-etl"


class ConfigCommand(Command):
    options = [
        option("--config", description="Path to config file to use.", default=None)
    ]

    def parse_config(self, config_path: str | Path | None) -> Config:
        if not config_path:
            config_path = Path(appdirs.user_config_dir(APP_NAME)) / "config.toml"

        return Config.from_file(config_path)


class PulseListenCommand(ConfigCommand):
    name = "pulse listen"
    description = "Listen on the specified pulse queue."
    arguments = [argument("queue", description="Pulse queue to listen on.")]

    def handle(self):
        config = self.parse_config(self.option("config"))
        queue = self.argument("name")

        loop = asyncio.get_event_loop()
        loop.run_until_complete(listen(config, queue))
        return 0


class PulseListCommand(ConfigCommand):
    name = "pulse list"
    description = "List configured pulse exchanges."

    def handle(self):
        config = self.parse_config(self.option("config"))

        for name, queue in config.pulse.queues.items():
            self.line(f"<fg=blue;options=bold>{name}</>")
            for key in sorted(asdict(queue)):
                value = getattr(queue, key)
                if isinstance(value, int | bool):
                    value = f"<fg=blue>{value}</>"
                elif isinstance(value, str):
                    value = f'<fg=light_red>"{value}"</>'
                self.line(f"<fg=dark_gray>{key}</>={value}")
            self.line("")
        return 0


def run():
    application = Application()
    application.add(PulseListenCommand())
    application.add(PulseListCommand())
    application.run()


if __name__ == "__main__":
    sys.exit(run())
