# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: validación contractual
# MAGIC
# MAGIC Los registros con errores de calidad ya fueron enviados a cuarentena. Este
# MAGIC notebook falla solamente si la tabla canónica viola su contrato: una clave
# MAGIC duplicada, un rango de dormitorios inválido o una fila sin manifest `success`.

# COMMAND ----------

# MAGIC %run ./config/parameters

# COMMAND ----------

from datetime import datetime

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text(
    "processing_date",
    "",
    "Fecha de snapshot YYYY-MM-DD (vacío = última fecha publicada en Silver)",
)


def _resolve_processing_date() -> str:
    requested_date = dbutils.widgets.get("processing_date").strip()
    if requested_date:
        return datetime.strptime(requested_date, "%Y-%m-%d").date().isoformat()

    latest = (
        spark.table(params["silver_snapshot_table"])
        .agg(F.max("ingest_date").alias("processing_date"))
        .first()["processing_date"]
    )
    if latest is None:
        raise ValueError("No hay snapshots Silver para validar.")
    return latest.isoformat()


processing_date = _resolve_processing_date()
snapshot = spark.table(params["silver_snapshot_table"]).where(
    F.col("ingest_date") == F.lit(processing_date).cast("date")
)
successful_runs = (
    spark.table(params["bronze_runs_table"])
    .where(F.lower(F.col("status")) == F.lit("success"))
    .select(F.col("run_id").alias("successful_run_id"))
    .dropDuplicates()
)

duplicate_keys = (
    snapshot.groupBy("ingest_date", "listing_id")
    .count()
    .where(F.col("count") > 1)
    .count()
)
invalid_bedroom_ranges = snapshot.where(
    F.col("bedrooms_min").isNull()
    | F.col("bedrooms_max").isNull()
    | (F.col("bedrooms_min") < 1)
    | (F.col("bedrooms_max") > 4)
    | (F.col("bedrooms_min") > F.col("bedrooms_max"))
).count()
rows_without_successful_manifest = snapshot.join(
    successful_runs,
    snapshot.run_id == successful_runs.successful_run_id,
    "left_anti",
).count()
view_rows = spark.table(params["silver_bedroom_options_view"]).where(
    F.col("ingest_date") == F.lit(processing_date).cast("date")
).count()
quarantined_rows = spark.table(params["silver_quarantine_table"]).where(
    F.col("ingest_date") == F.lit(processing_date).cast("date")
).count()

validation_metrics = spark.createDataFrame(
    [
        (processing_date, "canonical_rows", snapshot.count()),
        (processing_date, "duplicate_snapshot_keys", duplicate_keys),
        (processing_date, "invalid_bedroom_ranges", invalid_bedroom_ranges),
        (processing_date, "rows_without_successful_manifest", rows_without_successful_manifest),
        (processing_date, "bedroom_option_rows", view_rows),
        (processing_date, "quarantined_rule_violations", quarantined_rows),
    ],
    ["processing_date", "metric", "value"],
)
display(validation_metrics)

violations = {
    "duplicate_snapshot_keys": duplicate_keys,
    "invalid_bedroom_ranges": invalid_bedroom_ranges,
    "rows_without_successful_manifest": rows_without_successful_manifest,
}
failed_checks = {name: value for name, value in violations.items() if value > 0}

if failed_checks:
    raise AssertionError(f"Contrato Silver incumplido para {processing_date}: {failed_checks}")

print(f"Contrato Silver validado correctamente para {processing_date}.")
