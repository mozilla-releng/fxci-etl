import asyncio
import sys
from argparse import ArgumentParser
from pathlib import Path

import appdirs

from fxci_etl.config import Config
from fxci_etl.pulse import listen


def run(args: list[str] = sys.argv[1:]):
    appname = "fxci-etl"

    parser = ArgumentParser(prog=appname, description="Firefox-CI ETL")
    parser.add_argument(
        "config_path", nargs="?", default=None, help="Path to config file"
    )

    parser.parse_args(args).config_path
    config_path: str = parser.parse_args(args).config_path or appdirs.user_config_dir(
        appname
    )
    config = Config.from_file(Path(config_path) / "config.toml")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(listen(config.pulse))
