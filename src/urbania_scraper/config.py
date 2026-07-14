"""Scraper configuration: search parameters, paths, and runtime settings.

Everything is overridable via the CLI; a few knobs also read environment
variables so the same code runs locally and inside an Airflow worker.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlencode

# Repo root = three levels up from this file (src/urbania_scraper/config.py).
REPO_ROOT = Path(__file__).resolve().parents[2]

BASE_URL = "https://urbania.pe"

# Realistic browser context — reduces the chance of the 403 bot-block.
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
LOCALE = "es-PE"
TIMEZONE = "America/Lima"
VIEWPORT = {"width": 1366, "height": 900}


@dataclass
class ScrapeConfig:
    """Parameters describing one scrape run."""

    operation: str = "alquiler"          # alquiler | venta
    property_type: str = "departamentos"  # departamentos | casas | ...
    location: str = "lima"
    bedrooms: int = 2
    # Optional stable identifier supplied by an orchestrator. Airflow uses the
    # same value across task retries, so a retry rewrites the local artifact for
    # that logical run instead of generating a second run.
    run_id: str | None = None

    max_pages: int = 0                    # 0 = all pages until exhausted
    headless: bool = True

    # Politeness / anti-bot.
    min_delay: float = 2.0
    max_delay: float = 5.0
    nav_timeout_ms: int = 45_000
    max_retries: int = 3                  # per-page retries on 403/timeout

    # Output.
    output_root: Path = field(default_factory=lambda: REPO_ROOT / "data" / "bronze")

    def __post_init__(self) -> None:
        if self.bedrooms not in {1, 2, 3, 4}:
            raise ValueError("bedrooms must be one of: 1, 2, 3, 4")
        if self.max_pages < 0:
            raise ValueError("max_pages must be 0 or a positive integer")
        if self.min_delay < 0 or self.max_delay < self.min_delay:
            raise ValueError("delays must satisfy 0 <= min_delay <= max_delay")
        self.output_root = Path(self.output_root)

    def search_path(self) -> str:
        """The slug path, e.g. 'alquiler-de-departamentos-en-lima'."""
        return f"{self.operation}-de-{self.property_type}-en-{self.location}"

    def page_url(self, page: int) -> str:
        """Full search URL for the given 1-based page number."""
        query = urlencode({"bedroomsNumber": self.bedrooms, "page": page})
        return f"{BASE_URL}/buscar/{self.search_path()}?{query}"

    def search_params(self) -> dict:
        """Flat dict of the search params, stored on each bronze record."""
        return {
            "operation": self.operation,
            "property_type": self.property_type,
            "location": self.location,
            "bedrooms": self.bedrooms,
        }


def env_user_agent() -> str:
    return os.environ.get("URBANIA_USER_AGENT", DEFAULT_USER_AGENT)
