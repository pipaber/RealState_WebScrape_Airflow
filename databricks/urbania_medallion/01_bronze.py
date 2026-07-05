# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: aterrizar los listings crudos de urbania en Delta
# MAGIC Lee el archivo NDJSON elegido por `config/parameters`, lo marca con
# MAGIC metadata de ingesta, y lo agrega a la tabla bronze — deduplicado por
# MAGIC `_run_id` para que volver a correr este notebook sobre el mismo archivo
# MAGIC no haga nada.

# COMMAND ----------

# MAGIC %run ./config/parameters

# COMMAND ----------

from pyspark.sql import functions as F

catalog = params['catalog']
schema = params['bronze_schema']
bronze_table = params['bronze_table']
bronze_fqn = f"{catalog}.{schema}.{bronze_table}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# COMMAND ----------

raw_df = spark.read.json(params['source_path'])

bronze_df = (
    raw_df
    .withColumn("_bronze_source_file", F.col("_metadata.file_path"))
    .withColumn("_bronze_loaded_at", F.current_timestamp())
    # ingest_date se calcula a partir del timestamp de scrapeo y no de la
    # carpeta de partición, así funciona aunque se lea un solo archivo.
    .withColumn("ingest_date", F.to_date("_ingested_at"))
)

print(f"Se leyeron {bronze_df.count()} filas desde {params['source_path']}")
display(bronze_df)

# COMMAND ----------

if not spark.catalog.tableExists(bronze_fqn):
    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_fqn)
    )
    print(f"Se creó {bronze_fqn} con {bronze_df.count()} filas.")
else:
    existing_run_ids = spark.table(bronze_fqn).select("_run_id").distinct()
    new_rows_df = bronze_df.join(existing_run_ids, on="_run_id", how="left_anti")
    new_count = new_rows_df.count()
    if new_count == 0:
        print(f"El run_id ya está presente en {bronze_fqn}; no hay nada que agregar.")
    else:
        (
            new_rows_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(bronze_fqn)
        )
        print(f"Se agregaron {new_count} filas nuevas a {bronze_fqn}.")

# COMMAND ----------

display(spark.table(bronze_fqn))
