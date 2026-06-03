"""Urbania.pe rental-listing scraper — bronze layer.

Scrapes apartment listings from urbania.pe with Playwright (the site has bot
protection, so a real browser is required) and writes raw, minimally-transformed
records as JSON Lines in a Hive-style partition layout ready to be synced to
Azure Blob Storage by an Airflow DAG.
"""

__version__ = "0.1.0"
