# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: calidad, normalización y snapshot canónico
# MAGIC
# MAGIC Este notebook procesa una fecha de snapshot. Conserva la evidencia de
# MAGIC errores en `silver.listings_quarantine`, publica métricas de calidad y
# MAGIC deja una sola observación canónica por `ingest_date + listing_id`.
# MAGIC Reejecutarlo para la misma fecha reemplaza ese snapshot de forma
# MAGIC idempotente.

# COMMAND ----------

# MAGIC %run ./config/parameters

# COMMAND ----------

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Window, functions as F

# COMMAND ----------

dbutils.widgets.text(
    "processing_date",
    "",
    "Fecha de snapshot YYYY-MM-DD (vacío = último manifest exitoso)",
)


def _resolve_processing_date() -> str:
    """Valida la fecha solicitada o toma el último manifest exitoso."""
    requested_date = dbutils.widgets.get("processing_date").strip()
    if requested_date:
        return datetime.strptime(requested_date, "%Y-%m-%d").date().isoformat()

    latest = (
        spark.table(params["bronze_runs_table"])
        .where(
            (F.lower(F.col("status")) == F.lit("success"))
            & F.col("_partition_ingest_date").isNotNull()
        )
        .agg(F.max("_partition_ingest_date").alias("processing_date"))
        .first()["processing_date"]
    )
    if latest is None:
        raise ValueError("No hay manifests exitosos en Bronze para procesar Silver.")
    return latest.isoformat()


processing_date = _resolve_processing_date()
print(f"Procesando Silver para ingest_date={processing_date}")

# COMMAND ----------

RULE_DESCRIPTIONS = {
    "MANIFEST_NOT_SUCCESS": "El anuncio no tiene un manifest exitoso para el run.",
    "LISTING_ID_REQUIRED": "listing_id es obligatorio para publicar un snapshot.",
    "RUN_ID_REQUIRED": "_run_id es obligatorio para mantener trazabilidad.",
    "INGEST_DATE_INVALID": "La fecha de partición no coincide con la fecha procesada.",
    "BEDROOMS_OUT_OF_RANGE": "El rango de dormitorios debe estar entre 1 y 4.",
    "PATH_SEARCH_BEDROOM_MISMATCH": "El dormitorio de la partición no coincide con el parámetro de búsqueda.",
    "PRICE_NON_POSITIVE": "El precio interpretado debe ser mayor que cero cuando exista.",
    "AREA_NON_POSITIVE": "El área interpretada debe ser mayor que cero cuando exista.",
}


def _table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def _ensure_delta_table(df: DataFrame, table_name: str) -> None:
    """Crea una tabla Delta vacía con el schema del DataFrame si aún no existe."""
    if not _table_exists(table_name):
        df.limit(0).write.format("delta").saveAsTable(table_name)


def _replace_date_with_merge(
    source_df: DataFrame,
    target_table: str,
    merge_condition: str,
) -> None:
    """Sincroniza por MERGE una fecha completa, eliminando filas obsoletas de esa fecha."""
    _ensure_delta_table(source_df, target_table)
    (
        DeltaTable.forName(spark, target_table)
        .alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(
            condition=f"target.ingest_date = DATE('{processing_date}')"
        )
        .execute()
    )


def _area_decimal(text_column: Column) -> Column:
    """Convierte números de área usando coma o punto como separador decimal."""
    return F.when(
        F.length(text_column) > 0,
        F.regexp_replace(text_column, ",", ".").cast("double"),
    )


def _integer(text_column: Column) -> Column:
    return F.when(F.length(text_column) > 0, text_column.cast("int"))


def _rule_description(rule_column: Column) -> Column:
    expression = None
    for rule_id, description in RULE_DESCRIPTIONS.items():
        condition = rule_column == F.lit(rule_id)
        expression = (
            F.when(condition, F.lit(description))
            if expression is None
            else expression.when(condition, F.lit(description))
        )
    return expression.otherwise(F.lit("Regla de calidad no documentada."))


# COMMAND ----------

successful_runs = (
    spark.table(params["bronze_runs_table"])
    .where(
        (F.lower(F.col("status")) == F.lit("success"))
        & (F.col("_partition_ingest_date") == F.lit(processing_date).cast("date"))
    )
    .select(F.col("run_id").alias("successful_run_id"))
    .where(F.col("successful_run_id").isNotNull())
    .dropDuplicates()
)

if successful_runs.limit(1).count() == 0:
    raise ValueError(
        f"No existe un manifest success para {processing_date}; Silver no publicará datos incompletos."
    )

raw_for_date = (
    spark.table(params["bronze_listings_table"])
    .where(F.col("_partition_ingest_date") == F.lit(processing_date).cast("date"))
    .alias("raw")
)

source = (
    raw_for_date.join(
        successful_runs.alias("runs"),
        F.col("raw._run_id") == F.col("runs.successful_run_id"),
        "left",
    )
    .select("raw.*", F.col("runs.successful_run_id"))
)

# COMMAND ----------

price_text = F.regexp_extract(
    F.coalesce(F.col("price_raw"), F.lit("")),
    r"([0-9][0-9.,]*)",
    1,
)
parsed_price = F.when(
    F.length(price_text) > 0,
    # Urbania publishes rents as whole amounts; punctuation is a thousands separator.
    F.regexp_replace(price_text, r"[^0-9]", "").cast("double"),
)

area_min_text = F.regexp_extract(
    F.coalesce(F.col("area_raw"), F.lit("")),
    r"^\s*(\d+(?:[.,]\d+)?)",
    1,
)
area_max_range_text = F.regexp_extract(
    F.coalesce(F.col("area_raw"), F.lit("")),
    r"(?i)(?:a|-)\s*(\d+(?:[.,]\d+)?)",
    1,
)
bedrooms_min_text = F.regexp_extract(
    F.coalesce(F.col("bedrooms_raw"), F.lit("")),
    r"^\s*(\d+)",
    1,
)
bedrooms_max_range_text = F.regexp_extract(
    F.coalesce(F.col("bedrooms_raw"), F.lit("")),
    r"(?i)(?:a|-)\s*(\d+)",
    1,
)
bathrooms_text = F.regexp_extract(
    F.coalesce(F.col("bathrooms_raw"), F.lit("")),
    r"^\s*(\d+)",
    1,
)

candidates = (
    source.withColumn("ingest_date", F.col("_partition_ingest_date").cast("date"))
    .withColumn("listing_id", F.trim(F.col("listing_id")))
    .withColumn("run_id", F.coalesce(F.col("_run_id"), F.lit("missing_run_id")))
    .withColumn(
        "source_observation_key",
        F.coalesce(
            F.col("_bronze_observation_key"),
            F.sha2(
                F.concat_ws("||", F.col("run_id"), F.coalesce(F.col("listing_id"), F.col("url"))),
                256,
            ),
        ),
    )
    .withColumn("has_success_manifest", F.col("successful_run_id").isNotNull())
    .withColumn(
        "district",
        F.coalesce(F.initcap(F.lower(F.trim(F.col("district")))), F.lit("Unknown")),
    )
    .withColumn(
        "city",
        F.coalesce(F.initcap(F.lower(F.trim(F.col("city")))), F.lit("Lima")),
    )
    .withColumn("listing_type", F.lower(F.trim(F.col("listing_type"))))
    .withColumn(
        "currency",
        F.when(F.col("price_raw").rlike(r"(?i)US\$|USD"), F.lit("USD"))
        .when(F.col("price_raw").rlike(r"(?i)S/|PEN"), F.lit("PEN"))
        .when(F.upper(F.trim(F.col("currency"))).isin("PEN", "USD"), F.upper(F.trim(F.col("currency"))))
        .otherwise(F.lit(None).cast("string")),
    )
    .withColumn("price_amount", F.coalesce(F.col("price_min").cast("double"), parsed_price))
    .withColumn("area_min_m2", _area_decimal(area_min_text))
    .withColumn(
        "area_max_m2",
        F.coalesce(_area_decimal(area_max_range_text), F.col("area_min_m2")),
    )
    .withColumn("area_avg_m2", (F.col("area_min_m2") + F.col("area_max_m2")) / F.lit(2.0))
    .withColumn(
        "bedrooms_min",
        F.coalesce(
            _integer(bedrooms_min_text),
            F.col("_search_params.bedrooms"),
            F.col("_partition_bedrooms"),
        ),
    )
    .withColumn(
        "bedrooms_max",
        F.coalesce(_integer(bedrooms_max_range_text), F.col("bedrooms_min")),
    )
    .withColumn("bathrooms", _integer(bathrooms_text))
    .withColumn("observed_at", F.to_timestamp(F.col("_ingested_at")))
    .withColumn("source_file", F.col("_bronze_source_file"))
    .withColumn("raw_payload", F.to_json(F.col("_raw")))
    .withColumn(
        "price_per_m2",
        F.when(
            (F.col("price_amount") > 0) & (F.col("area_avg_m2") > 0),
            F.col("price_amount") / F.col("area_avg_m2"),
        ),
    )
    .withColumn(
        "_rule_candidates",
        F.array(
            F.when(~F.col("has_success_manifest"), F.lit("MANIFEST_NOT_SUCCESS")),
            F.when(F.col("listing_id").isNull() | (F.length(F.col("listing_id")) == 0), F.lit("LISTING_ID_REQUIRED")),
            F.when(F.col("_run_id").isNull() | (F.length(F.trim(F.col("_run_id"))) == 0), F.lit("RUN_ID_REQUIRED")),
            F.when(F.col("ingest_date").isNull() | (F.col("ingest_date") != F.lit(processing_date).cast("date")), F.lit("INGEST_DATE_INVALID")),
            F.when(
                F.col("bedrooms_min").isNull()
                | F.col("bedrooms_max").isNull()
                | (F.col("bedrooms_min") < 1)
                | (F.col("bedrooms_max") > 4)
                | (F.col("bedrooms_min") > F.col("bedrooms_max")),
                F.lit("BEDROOMS_OUT_OF_RANGE"),
            ),
            F.when(
                F.col("_search_params.bedrooms").isNotNull()
                & F.col("_partition_bedrooms").isNotNull()
                & (F.col("_search_params.bedrooms") != F.col("_partition_bedrooms")),
                F.lit("PATH_SEARCH_BEDROOM_MISMATCH"),
            ),
            F.when(F.col("price_amount").isNotNull() & (F.col("price_amount") <= 0), F.lit("PRICE_NON_POSITIVE")),
            F.when(
                (F.col("area_min_m2").isNotNull() & (F.col("area_min_m2") <= 0))
                | (F.col("area_max_m2").isNotNull() & (F.col("area_max_m2") <= 0)),
                F.lit("AREA_NON_POSITIVE"),
            ),
        ),
    )
    .withColumn("_failed_rules", F.expr("filter(_rule_candidates, rule -> rule IS NOT NULL)"))
    .drop("_rule_candidates")
)

# COMMAND ----------

invalid_records = candidates.where(F.size(F.col("_failed_rules")) > 0)
valid_records = candidates.where(F.size(F.col("_failed_rules")) == 0)

quarantine_rows = (
    invalid_records.select(
        "ingest_date",
        "listing_id",
        "run_id",
        "source_observation_key",
        F.explode(F.col("_failed_rules")).alias("rule_id"),
        "source_file",
        "raw_payload",
        F.current_timestamp().alias("quarantined_at"),
    )
    .withColumn("rule_description", _rule_description(F.col("rule_id")))
    .select(
        "ingest_date",
        "listing_id",
        "run_id",
        "source_observation_key",
        "rule_id",
        "rule_description",
        "source_file",
        "raw_payload",
        "quarantined_at",
    )
)

deduplication_window = Window.partitionBy("ingest_date", "listing_id").orderBy(
    F.col("observed_at").desc_nulls_last(),
    F.col("_bronze_loaded_at").desc_nulls_last(),
    F.col("run_id").desc(),
)

snapshot_rows = (
    valid_records.withColumn("_dedup_rank", F.row_number().over(deduplication_window))
    .where(F.col("_dedup_rank") == 1)
    .select(
        "ingest_date",
        "listing_id",
        "listing_type",
        "url",
        "district",
        "city",
        "bedrooms_min",
        "bedrooms_max",
        "bathrooms",
        "area_min_m2",
        "area_max_m2",
        "area_avg_m2",
        "currency",
        "price_amount",
        "price_per_m2",
        "features",
        "description",
        "publisher",
        "run_id",
        "source_file",
        "source_observation_key",
        "observed_at",
        F.col("_bronze_loaded_at").alias("bronze_loaded_at"),
        F.col("price_amount").isNotNull().alias("has_price"),
        F.col("area_avg_m2").isNotNull().alias("has_area"),
        F.current_timestamp().alias("updated_at"),
    )
)

# COMMAND ----------

rule_definitions = spark.createDataFrame(
    [(rule_id, description) for rule_id, description in RULE_DESCRIPTIONS.items()],
    ["rule_id", "rule_description"],
)

evaluated_by_run = candidates.groupBy("ingest_date", "run_id").agg(
    F.count(F.lit(1)).alias("records_evaluated")
)
invalid_by_rule = quarantine_rows.groupBy("ingest_date", "run_id", "rule_id").agg(
    F.countDistinct("source_observation_key").alias("invalid_records")
)

quality_results = (
    evaluated_by_run.crossJoin(rule_definitions)
    .join(invalid_by_rule, ["ingest_date", "run_id", "rule_id"], "left")
    .fillna({"invalid_records": 0})
    .withColumn("valid_records", F.col("records_evaluated") - F.col("invalid_records"))
    .withColumn(
        "error_pct",
        F.round(F.col("invalid_records") / F.col("records_evaluated") * F.lit(100.0), 2),
    )
    .withColumn("result", F.when(F.col("invalid_records") > 0, F.lit("FAIL")).otherwise(F.lit("PASS")))
    .withColumn("calculated_at", F.current_timestamp())
    .select(
        "ingest_date",
        "run_id",
        "rule_id",
        "rule_description",
        "records_evaluated",
        "valid_records",
        "invalid_records",
        "error_pct",
        "result",
        "calculated_at",
    )
)

# COMMAND ----------

_replace_date_with_merge(
    snapshot_rows,
    params["silver_snapshot_table"],
    "target.ingest_date = source.ingest_date AND target.listing_id = source.listing_id",
)
_replace_date_with_merge(
    quarantine_rows,
    params["silver_quarantine_table"],
    "target.ingest_date = source.ingest_date "
    "AND target.source_observation_key = source.source_observation_key "
    "AND target.rule_id = source.rule_id",
)
_replace_date_with_merge(
    quality_results,
    params["silver_quality_results_table"],
    "target.ingest_date = source.ingest_date "
    "AND target.run_id = source.run_id "
    "AND target.rule_id = source.rule_id",
)

print(f"Silver publicado para {processing_date}.")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
          COUNT(*) AS canonical_listings,
          COUNT(DISTINCT listing_id) AS unique_listing_ids,
          SUM(CASE WHEN has_price THEN 1 ELSE 0 END) AS listings_with_price,
          SUM(CASE WHEN has_area THEN 1 ELSE 0 END) AS listings_with_area
        FROM {params['silver_snapshot_table']}
        WHERE ingest_date = DATE('{processing_date}')
        """
    )
)

display(
    spark.sql(
        f"""
        SELECT rule_id, SUM(invalid_records) AS invalid_records, MAX(result) AS result
        FROM {params['silver_quality_results_table']}
        WHERE ingest_date = DATE('{processing_date}')
        GROUP BY rule_id
        ORDER BY rule_id
        """
    )
)
