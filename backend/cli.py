"""Elephant backend CLI.

Usage (from the backend/ directory):

    python cli.py scrape          # trigger a leaderboard scrape
    python cli.py scrape --force  # bypass the 1-hour rate-limit cache
"""

import argparse
import asyncio
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def cmd_scrape(args: argparse.Namespace) -> None:
    from app.db import SessionLocal
    from app.services.leaderboard_scraper import scraper, _last_scrape_time  # noqa: F401

    # Optionally bypass rate-limit for manual runs
    if args.force:
        import app.services.leaderboard_scraper as mod
        mod._last_scrape_time = None

    async def _run():
        db = SessionLocal()
        try:
            return await scraper.scrape(db)
        finally:
            db.close()

    count = asyncio.run(_run())
    print(f"Scrape complete: {count} traders upserted.")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="elephant-cli",
        description="Elephant backend management commands",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")

    scrape_parser = subparsers.add_parser("scrape", help="Trigger a Kalshi leaderboard scrape")
    scrape_parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass the 1-hour rate-limit cache",
    )
    scrape_parser.set_defaults(func=cmd_scrape)

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
