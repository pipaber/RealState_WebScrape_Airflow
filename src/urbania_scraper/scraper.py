"""Playwright-based scraper for urbania.pe search results.

The site returns HTTP 403 to plain requests (bot protection) and renders results
server-side into the DOM with stable ``data-qa`` markers — there is no usable
listings JSON endpoint (only third-party analytics XHRs). So we drive a real
Chromium browser, wait for the result cards, and extract each card in-page.

``networkidle`` never settles on this site (continuous analytics/ads/websocket
traffic), so we navigate with ``domcontentloaded`` and then poll for the posting
cards to appear.

Cloudflare serves an "Un momento…" interstitial when the same browser context
re-navigates in rapid succession, so we use a **fresh browser context per page**
(verified to sidestep the challenge). Pagination advances the ``page`` query
param until a page yields no new listings (dedup by ``data-id``) or ``max_pages``
is reached. A detected challenge is retried with exponential backoff; a genuine
empty page ends pagination.
"""

from __future__ import annotations

import asyncio
import json
import random
from dataclasses import dataclass

from playwright.async_api import async_playwright

try:  # optional anti-bot hardening
    from playwright_stealth import stealth_async
except Exception:  # pragma: no cover - optional dep
    stealth_async = None

from . import parse
from .config import LOCALE, TIMEZONE, VIEWPORT, ScrapeConfig, env_user_agent
from .models import BronzeListing
from .storage import BronzeWriter, make_run_id, utc_now_iso

# CSS selector for a result card (attribute starts with "posting", e.g.
# data-qa="posting PROPERTY" / "posting DEVELOPMENT").
CARD_SELECTOR = "[data-qa^='posting'][data-id]"

# In-page extractor. Reads the stable data-qa markers verified against the live
# DOM: PRICE, FEATURES (area/rooms/units lumped together), LOCATION (district,
# city) whose previous sibling holds the street line, DESCRIPTION, the amenity
# pills, the gallery image, and the card-level data-* attributes.
_DOM_EXTRACT_JS = r"""
() => {
  const txt = (el) => {
    if (!el) return null;
    const t = el.textContent.replace(/\s+/g, ' ').trim();
    return t === '' ? null : t;
  };
  const cards = Array.from(document.querySelectorAll("[data-qa^='posting'][data-id]"));
  return cards.map((card) => {
    const loc = card.querySelector("[data-qa='POSTING_CARD_LOCATION']");
    const street = loc && loc.previousElementSibling
      ? txt(loc.previousElementSibling) : null;
    const amenities = Array.from(
      card.querySelectorAll("[class*='pill-item-feature']")
    ).map(txt).filter(Boolean);
    const img = card.querySelector('img');
    return {
      id: card.getAttribute('data-id'),
      posting_type: card.getAttribute('data-posting-type'),
      url: card.getAttribute('data-to-posting'),
      price: txt(card.querySelector("[data-qa='POSTING_CARD_PRICE']")),
      maintenance: txt(card.querySelector("[data-qa='expensas']")),
      features_text: txt(card.querySelector("[data-qa='POSTING_CARD_FEATURES']")),
      street: street,
      location: txt(loc),
      description: txt(card.querySelector("[data-qa='POSTING_CARD_DESCRIPTION']")),
      publisher: txt(card.querySelector("[data-qa='POSTING_CARD_PUBLISHER']")),
      amenities: amenities,
      image: img ? img.src : null,
    };
  });
}
"""


@dataclass
class PageResult:
    listings: list[BronzeListing]
    method: str  # "dom" | "none"


class UrbaniaScraper:
    def __init__(self, cfg: ScrapeConfig):
        self.cfg = cfg

    async def run(self) -> dict:
        """Execute the full scrape; returns a summary dict."""
        run_id = make_run_id()
        started_at = utc_now_iso()
        writer = BronzeWriter(self.cfg, run_id=run_id).open()

        seen_ids: set[str] = set()
        pages_scraped = 0
        status = "success"

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.cfg.headless)
            try:
                page_num = 1
                while self.cfg.max_pages == 0 or page_num <= self.cfg.max_pages:
                    result = await self._scrape_page(browser, page_num, run_id)
                    new_this_page = 0
                    for listing in result.listings:
                        key = listing.listing_id or listing.url or json.dumps(
                            listing.raw, sort_keys=True
                        )
                        if key in seen_ids:
                            continue
                        seen_ids.add(key)
                        writer.write(listing)
                        new_this_page += 1

                    pages_scraped += 1
                    print(
                        f"[page {page_num}] method={result.method} "
                        f"found={len(result.listings)} new={new_this_page} "
                        f"total={writer.count}"
                    )

                    # Stop when a page yields no new listings (last page reached).
                    if new_this_page == 0:
                        break

                    page_num += 1
                    await asyncio.sleep(
                        random.uniform(self.cfg.min_delay, self.cfg.max_delay)
                    )
            except Exception as exc:  # write what we have, then record failure
                status = f"error: {exc}"
                print(f"[error] {exc}")
            finally:
                await browser.close()

        writer.close(pages_scraped=pages_scraped, started_at=started_at, status=status)
        return {
            "run_id": run_id,
            "status": status,
            "records": writer.count,
            "pages": pages_scraped,
            "output": str(writer.data_path),
            "manifest": str(writer.manifest_path),
        }

    async def _scrape_page(self, browser, page_num: int, run_id: str) -> PageResult:
        """Scrape one page in a fresh context (with retries on bot challenges)."""
        meta = {
            "_ingested_at": utc_now_iso(),
            "_source_url": self.cfg.page_url(page_num),
            "_page_num": page_num,
            "_run_id": run_id,
            "_search_params": self.cfg.search_params(),
        }

        last_exc: Exception | None = None
        for attempt in range(1, self.cfg.max_retries + 1):
            # Fresh context per attempt: a new context sidesteps the Cloudflare
            # "Un momento…" challenge that a re-navigating context triggers.
            context = await browser.new_context(
                user_agent=env_user_agent(),
                locale=LOCALE,
                timezone_id=TIMEZONE,
                viewport=VIEWPORT,
            )
            page = await context.new_page()
            if stealth_async is not None:
                await stealth_async(page)
            try:
                await page.goto(
                    self.cfg.page_url(page_num),
                    wait_until="domcontentloaded",
                    timeout=self.cfg.nav_timeout_ms,
                )
                cards = await self._extract_cards(page)
                if cards:
                    listings = [
                        BronzeListing(**parse.listing_from_card_dict(c, meta=meta))
                        for c in cards
                    ]
                    return PageResult(listings=listings, method="dom")

                # No cards. Tell apart a bot challenge (retry) from a genuine
                # empty page past the last result (stop pagination).
                title = (await page.title()).lower()
                if any(t in title for t in ("momento", "moment", "just a")):
                    raise RuntimeError(f"bot challenge (title={title!r})")
                return PageResult(listings=[], method="none")
            except Exception as exc:
                last_exc = exc
                backoff = 2 ** attempt
                print(f"[page {page_num}] attempt {attempt} failed: {exc}; "
                      f"retry in {backoff}s")
                await asyncio.sleep(backoff)
            finally:
                await context.close()

        raise RuntimeError(
            f"page {page_num} failed after {self.cfg.max_retries} attempts: {last_exc}"
        )

    async def _extract_cards(self, page) -> list[dict] | None:
        """Poll up to ~20s for cards to render; return the extracted dicts or None."""
        waited = 0.0
        while waited < 20:
            count = await page.evaluate(
                f"() => document.querySelectorAll(\"{CARD_SELECTOR}\").length"
            )
            if count:
                return await page.evaluate(_DOM_EXTRACT_JS)
            await page.wait_for_timeout(2000)
            waited += 2
        return None


async def scrape(cfg: ScrapeConfig) -> dict:
    return await UrbaniaScraper(cfg).run()
