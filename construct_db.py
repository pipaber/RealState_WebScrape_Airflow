"""Deprecated stub — superseded by the urbania_scraper package.

The original Playwright scaffold here grew into a proper package:

    src/urbania_scraper/
        scraper.py   - Playwright navigation, XHR interception, pagination
        parse.py     - raw API/DOM -> BronzeListing mapping
        storage.py   - NDJSON bronze partition writer
        cli.py       - command-line entrypoint

Run it with:

    python main.py --bedrooms 2 --max-pages 1
    # or
    python -m urbania_scraper.cli --bedrooms 2

This file is kept only to redirect anyone who imported it before.
"""

from urbania_scraper.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
