"""Command-line entrypoint for the bronze scraper.

Examples:
    python -m urbania_scraper.cli --bedrooms 2 --max-pages 1
    urbania-scrape --operation alquiler --property departamentos --location lima
"""

from __future__ import annotations

import argparse
import asyncio
import json

from .config import ScrapeConfig
from .scraper import scrape


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Scrape urbania.pe listings (bronze layer).")
    p.add_argument("--operation", default="alquiler", help="alquiler | venta")
    p.add_argument("--property", dest="property_type", default="departamentos",
                   help="property type slug, e.g. departamentos")
    p.add_argument("--location", default="lima")
    p.add_argument("--bedrooms", type=int, default=2)
    p.add_argument("--max-pages", type=int, default=0,
                   help="0 = scrape all pages until exhausted")
    p.add_argument("--no-headless", dest="headless", action="store_false",
                   help="run with a visible browser window (debugging)")
    p.set_defaults(headless=True)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = ScrapeConfig(
        operation=args.operation,
        property_type=args.property_type,
        location=args.location,
        bedrooms=args.bedrooms,
        max_pages=args.max_pages,
        headless=args.headless,
    )
    summary = asyncio.run(scrape(cfg))
    print("\n=== scrape summary ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
