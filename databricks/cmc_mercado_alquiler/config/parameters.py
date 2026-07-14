# Databricks notebook source
# MAGIC %md
# MAGIC # Parámetros del Data Product `cmc_mercado_alquiler`
# MAGIC
# MAGIC Los widgets permiten ejecutar los mismos notebooks manualmente o mediante
# MAGIC Lakeflow Jobs sin codificar rutas o nombres de tablas en la lógica ETL.

# COMMAND ----------

DEFAULT_CATALOG = "g101_cmc_mercado_alquiler"
DEFAULT_RAW_SOURCE_PATH = (
    "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G1/source=urbania/"
)
DEFAULT_CHECKPOINT_ROOT = (
    "abfss://datalake@stdemdsai.dfs.core.windows.net/checkpoints/"
    "G1/cmc_mercado_alquiler/"
)

dbutils.widgets.text("catalog", DEFAULT_CATALOG, "Unity Catalog")
dbutils.widgets.text("raw_source_path", DEFAULT_RAW_SOURCE_PATH, "Ruta raw de Urbania")
dbutils.widgets.text("checkpoint_root", DEFAULT_CHECKPOINT_ROOT, "Raíz de checkpoints")

catalog = dbutils.widgets.get("catalog").strip()
raw_source_path = dbutils.widgets.get("raw_source_path").rstrip("/") + "/"
checkpoint_root = dbutils.widgets.get("checkpoint_root").rstrip("/") + "/"

if not catalog:
    raise ValueError("El parámetro 'catalog' no puede estar vacío.")

params = {
    "catalog": catalog,
    "raw_source_path": raw_source_path,
    "bronze_listings_table": f"{catalog}.bronze.listings_raw",
    "bronze_runs_table": f"{catalog}.bronze.ingestion_runs",
    "silver_snapshot_table": f"{catalog}.silver.listings_snapshot",
    "silver_quarantine_table": f"{catalog}.silver.listings_quarantine",
    "silver_quality_results_table": f"{catalog}.silver.data_quality_results",
    "silver_bedroom_options_view": f"{catalog}.silver.vw_listing_bedroom_options",
    "gold_market_daily_table": f"{catalog}.gold.market_daily_by_district",
    "gold_listing_latest_table": f"{catalog}.gold.listing_latest",
    "gold_listing_changes_table": f"{catalog}.gold.listing_change_daily",
    "listings_schema_location": f"{checkpoint_root}schemas/urbania_listings/",
    "listings_checkpoint_location": f"{checkpoint_root}bronze/urbania_listings/",
    "manifests_schema_location": f"{checkpoint_root}schemas/urbania_manifests/",
    "manifests_checkpoint_location": f"{checkpoint_root}bronze/urbania_manifests/",
}

print("Parámetros cargados para cmc_mercado_alquiler")
display(spark.createDataFrame([(key, value) for key, value in params.items()], ["key", "value"]))
