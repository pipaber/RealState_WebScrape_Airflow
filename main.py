"""Thin wrapper so `python main.py ...` runs the scraper CLI.

The real entrypoint lives in src/urbania_scraper/cli.py.
"""

import sys
from pathlib import Path

# Make the src/ layout importable when running this file directly.
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from urbania_scraper.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
