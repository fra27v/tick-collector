import argparse
import logging
import signal
from .config import load
from .collector import Collector
from .adapter import IBAdapter
from .storage import compact


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("collect").add_argument("--config", required=True)
    p = sub.add_parser("compact")
    p.add_argument("--config", required=True)
    p.add_argument("--delete-originals", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(name)s %(message)s")
    logging.Formatter.converter = __import__("time").gmtime
    try:
        config = load(args.config)
        if args.command == "compact":
            compact(config, args.delete_originals)
        else:
            collector = Collector(config, IBAdapter)
            def stop(signum, frame):
                collector.stop.set()
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, stop)
            collector.run()
    except Exception:
        logging.exception("Command failed")
        return 1
    return 0
