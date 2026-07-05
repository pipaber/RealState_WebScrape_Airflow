# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: mercado de alquiler de Lima por distrito y día
# MAGIC Agrega silver en las métricas que pide el brief del proyecto: niveles
# MAGIC de precio, precio por m² y oferta por distrito, trackeables en el
# MAGIC tiempo con `ingest_date`. Se recalcula por completo desde silver en
# MAGIC cada corrida.

# COMMAND ----------

# MAGIC %run ./config/parameters

# COMMAND ----------

from pyspark.sql import functions as F

catalog = params['catalog']
silver_fqn = f"{catalog}.{params['silver_schema']}.{params['silver_table']}"
gold_fqn = f"{catalog}.{params['gold_schema']}.{params['gold_table']}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{params['gold_schema']}`")

silver_df = spark.table(silver_fqn)

# COMMAND ----------

gold_df = (
    silver_df
    .groupBy("district_clean", "ingest_date")
    .agg(
        F.count("*").alias("listing_count"),
        F.round(F.avg("price_amount"), 2).alias("avg_price"),
        F.round(F.min("price_amount"), 2).alias("min_price"),
        F.round(F.max("price_amount"), 2).alias("max_price"),
        F.round(F.avg("area_avg_m2"), 2).alias("avg_area_m2"),
        F.round(F.avg("price_per_m2"), 2).alias("avg_price_per_m2"),
        F.round(F.avg((F.col("bedrooms_min") + F.col("bedrooms_max")) / 2), 2).alias("avg_bedrooms"),
    )
    .withColumnRenamed("district_clean", "district")
    .orderBy("ingest_date", "district")
)

display(gold_df)

# COMMAND ----------

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_fqn)
)
print(f"Se escribieron {gold_df.count()} filas en {gold_fqn}.")
