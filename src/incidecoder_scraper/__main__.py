"""Command line entry point for the INCIDecoder scraper."""

from __future__ import annotations

import argparse
import logging
import sys

from .scraper import IncidecoderScraper
from .storage import DataStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape INCIDecoder product data")
    parser.add_argument(
        "--database",
        "-d",
        default="incidecoder.duckdb",
        help="Path to the DuckDB (or SQLite fallback) database file.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional number of products to scrape (for testing).",
    )
    parser.add_argument(
        "--strategy",
        choices=["auto", "sitemap", "brands"],
        default="auto",
        help="Product discovery strategy to use.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Do not skip products that are already stored in the database.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (e.g. DEBUG, INFO, WARNING).",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=1.0,
        help="Seconds to wait between HTTP requests (fractional values allowed).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    scraper = IncidecoderScraper()
    scraper.http.throttle_seconds = args.throttle
    store = DataStore(args.database)
    try:
        scraper.scrape(store, limit=args.limit, strategy=args.strategy, resume=not args.no_resume)
    finally:
        store.close()
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())
