"""Map a DOM-scraped card (plain strings from the in-page JS) into a BronzeListing.

Urbania renders results server-side with stable ``data-qa`` markers, so we scrape
the DOM rather than an API (there is no usable listings JSON endpoint — only
third-party analytics XHRs). The in-page JS in ``scraper.py`` produces one dict
per card; here we map it to BronzeListing kwargs.

The site mixes two card types:
  * PROPERTY    — a single unit:        "59 m² tot.2 dorm.1 baño"
  * DEVELOPMENT — a project with ranges: "160 un.1 a 2 dorm.45 a 60 m² tot."

We keep the raw feature string in ``_raw`` and additionally split it into
``area_raw`` / ``bedrooms_raw`` / ``bathrooms_raw`` / ``units_raw`` by their unit
tokens. That split is structural (not numeric normalization), which is acceptable
at the bronze layer; turning "45 a 60 m²" into numbers is a silver-layer job.
"""

from __future__ import annotations

import re

from .config import BASE_URL

# Each token captures a value that may itself be a range like "1 a 2".
_NUM = r"\d+(?:\s*a\s*\d+)?"
_AREA_RE = re.compile(rf"({_NUM})\s*m²")
_BEDROOMS_RE = re.compile(rf"({_NUM})\s*dorm")
_BATHROOMS_RE = re.compile(rf"({_NUM})\s*baño")
_UNITS_RE = re.compile(rf"({_NUM})\s*un\.")


def _search(pattern: re.Pattern, text: str | None) -> str | None:
    if not text:
        return None
    m = pattern.search(text)
    return m.group(1).strip() if m else None


def _split_location(loc: str | None) -> tuple[str | None, str | None]:
    """'Rímac, Lima' -> ('Rímac', 'Lima'). Tolerates missing/extra commas."""
    if not loc:
        return None, None
    parts = [p.strip() for p in loc.split(",") if p.strip()]
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[-1]


def _abs_url(url: str | None) -> str | None:
    if not url:
        return None
    if url.startswith("http"):
        return url
    return BASE_URL + url


def listing_from_card_dict(card: dict, *, meta: dict) -> dict:
    """Map one DOM card dict to BronzeListing input kwargs (alias keys)."""
    features_text = card.get("features_text")
    district, city = _split_location(card.get("location"))

    return {
        "listing_id": card.get("id"),
        "listing_type": card.get("posting_type"),  # DEVELOPMENT | PROPERTY
        "url": _abs_url(card.get("url")),
        "price_raw": card.get("price"),
        "maintenance_raw": card.get("maintenance"),
        "area_raw": _search(_AREA_RE, features_text),
        "bedrooms_raw": _search(_BEDROOMS_RE, features_text),
        "bathrooms_raw": _search(_BATHROOMS_RE, features_text),
        "units_raw": _search(_UNITS_RE, features_text),
        "street": card.get("street"),
        "district": district,
        "city": city,
        "address_raw": card.get("location"),
        "features": card.get("amenities") or [],
        "description": card.get("description"),
        "image_url": card.get("image"),
        "publisher": card.get("publisher"),
        "_raw": card,
        "_extraction_method": "dom",
        **meta,
    }
