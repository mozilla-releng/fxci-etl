import argparse
import asyncio
import sys

from fxci_etl.pulse import listen


def run(args=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", help="PulseGuardian username")
    parser.add_argument("--password", help="PulseGuardian password")

    args = parser.parse_args(args)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(listen())
