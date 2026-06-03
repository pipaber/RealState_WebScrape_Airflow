"""Bronze-layer record schema.

Bronze = raw, minimally transformed. Listing fields are kept as the original
strings the site shows ("S/ 2,750", "45 a 60 m²", "1 a 2 dorm.") so nothing is
lost; parsing those into numbers belongs to a future silver layer. Every field
is optional because the site mixes new-development ("proyecto") cards that show
ranges with individual-unit cards that show single values, and markup changes
over time — a missing field must never crash a run.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class BronzeListing(BaseModel):
    """One scraped listing card, raw."""

    # --- Listing identity ---
    listing_id: str | None = None
    listing_type: str | None = None  # e.g. "proyecto" (development) vs "usado"
    url: str | None = None

    # --- Price (raw strings preserved) ---
    price_raw: str | None = None       # "S/ 2,750" / "Departamentos desde S/ 2,750"
    maintenance_raw: str | None = None  # "S/ 120 Mantenimiento" (expensas)
    currency: str | None = None        # "PEN" / "USD" if exposed
    price_min: float | None = None     # numeric minimum, only if exposed

    # --- Physical attributes (raw) ---
    area_raw: str | None = None      # "45 a 60 m²"
    bedrooms_raw: str | None = None  # "1 a 2 dorm."
    bathrooms_raw: str | None = None
    units_raw: str | None = None     # "160 un." (developments only)

    # --- Location (raw) ---
    address_raw: str | None = None
    street: str | None = None        # "Av. Juan de Arona 110"
    district: str | None = None      # "San Isidro"
    city: str | None = None          # "Lima"

    # --- Extras ---
    features: list[str] = Field(default_factory=list)  # Gimnasio, Piscina, ...
    description: str | None = None
    image_url: str | None = None
    publisher: str | None = None

    # --- Ingestion metadata (prefixed with _ to mark it as pipeline-added) ---
    ingested_at: str = Field(..., alias="_ingested_at")          # UTC ISO-8601
    source_url: str | None = Field(None, alias="_source_url")
    page_num: int | None = Field(None, alias="_page_num")
    run_id: str = Field(..., alias="_run_id")
    search_params: dict = Field(default_factory=dict, alias="_search_params")
    extraction_method: str | None = Field(None, alias="_extraction_method")  # "api"|"dom"
    raw: dict[str, Any] | None = Field(None, alias="_raw")  # original API object

    model_config = {
        "populate_by_name": True,   # accept both alias and field name on input
        "extra": "allow",           # tolerate unexpected fields rather than fail
    }

    def to_jsonl_dict(self) -> dict:
        """Serialize using aliases so the _-prefixed metadata keys are preserved."""
        return self.model_dump(by_alias=True, exclude_none=False)
