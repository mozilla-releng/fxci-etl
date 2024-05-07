import asyncio
import sys
from argparse import ArgumentParser
from pathlib import Path

import appdirs

from fxci_etl.config import Config
from fxci_etl.pulse.listen import listen


async def run_pulse(config: Config):
    return await listen(config)


def run(args: list[str] = sys.argv[1:]):
    appname = "fxci-etl"

    parser = ArgumentParser(prog=appname, description="Firefox-CI ETL")
    parser.add_argument(
        "--config",
        dest="config_path",
        nargs="?",
        default=None,
        help="Path to config file",
    )

    subparsers = parser.add_subparsers(help="Commands")
    pulse_parser = subparsers.add_parser("pulse")
    pulse_parser.set_defaults(func=run_pulse)

    ns = parser.parse_args(args)
    config_path: str = ns.config_path or appdirs.user_config_dir(appname)
    config = Config.from_file(Path(config_path) / "config.toml")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(ns.func(config))
