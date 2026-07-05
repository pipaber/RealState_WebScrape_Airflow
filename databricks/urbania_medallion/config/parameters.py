# Databricks notebook source
# MAGIC %md
# MAGIC # Config: pipeline parameters
# MAGIC Single source of truth for the medallion pipeline. Loaded by every layer
# MAGIC notebook via `%run ./config/parameters`.
# MAGIC
# MAGIC `source_path` is discovered automatically: it walks the Hive-style
# MAGIC partitions under `raw/airflow/G1/source=urbania/` and picks the most
# MAGIC recently modified `.jsonl` file, so you don't have to hardcode a run_id
# MAGIC or ingest_date by hand.

# COMMAND ----------

BASE_RAW_PATH = "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G1/source=urbania/"


def _find_latest_jsonl(path: str):
    """Recursively walk the Hive-style partitions and return the newest .jsonl FileInfo."""
    latest = None
    for entry in dbutils.fs.ls(path):
        if entry.isDir():
            candidate = _find_latest_jsonl(entry.path)
        elif entry.name.endswith(".jsonl"):
            candidate = entry
        else:
            candidate = None
        if candidate is not None and (latest is None or candidate.modificationTime > latest.modificationTime):
            latest = candidate
    return latest


_latest_file = _find_latest_jsonl(BASE_RAW_PATH)
if _latest_file is None:
    raise FileNotFoundError(f"No .jsonl files found under {BASE_RAW_PATH}")

params = {
    'catalog': 'g101_catalog',
    'prefix': 'pipaber',
    'source_path': _latest_file.path,
    'schema': 'pipaber',
    'bronze_table': 'bronze_urbania',
    'silver_table': 'silver_urbania',
    'gold_table': 'gold_urbania',
}
print('Dictionary imported: params')
