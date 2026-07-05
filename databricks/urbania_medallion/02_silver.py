# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean and type the bronze urbania listings
# MAGIC Bronze keeps every field as the raw string the site shows (`"S/ 2,750"`,
# MAGIC `"45 a 60 m²"`, `"1 a 2 dorm."`). Silver parses those into numeric
# MAGIC columns. Recomputed in full from bronze on every run — bronze is the
# MAGIC source of truth and this table is small enough not to need incremental
# MAGIC merge logic yet.

# COMMAND ----------

# MAGIC %run ./config/parameters

# COMMAND ----------

from pyspark.sql import functions as F

catalog = params['catalog']
schema = params['schema']
bronze_fqn = f"{catalog}.{schema}.{params['bronze_table']}"
silver_fqn = f"{catalog}.{schema}.{params['silver_table']}"

bronze_df = spark.table(bronze_fqn)


def _numeric(raw_col, pattern):
    """Extract the first numeric match, strip thousands commas, cast to double."""
    extracted = F.regexp_extract(raw_col, pattern, 1)
    return F.when(extracted == "", None).otherwise(
        F.regexp_replace(extracted, ",", "").cast("double")
    )


NUMBER = r"(\d+(?:[.,]\d+)*)"

# COMMAND ----------

silver_df = (
    bronze_df
    # --- price: "S/ 2,750" / "Departamentos desde S/ 2,750" / "US$ 1,200" ---
    .withColumn(
        "currency_clean",
        F.when(F.col("price_raw").rlike(r"US\$"), F.lit("USD"))
         .when(F.col("price_raw").rlike(r"S/\.?"), F.lit("PEN"))
         .otherwise(F.coalesce(F.col("currency"), F.lit("PEN"))),
    )
    .withColumn("price_amount", F.coalesce(F.col("price_min"), _numeric(F.col("price_raw"), NUMBER)))
    .withColumn("maintenance_amount", _numeric(F.col("maintenance_raw"), NUMBER))
    # --- ranges: "45 a 60 m²" / "1 a 2 dorm." — max falls back to min when no range ---
    .withColumn("area_min_m2", _numeric(F.col("area_raw"), NUMBER))
    .withColumn(
        "area_max_m2",
        F.coalesce(_numeric(F.col("area_raw"), r"a\s*" + NUMBER), F.col("area_min_m2")),
    )
    .withColumn("bedrooms_min", _numeric(F.col("bedrooms_raw"), NUMBER).cast("int"))
    .withColumn(
        "bedrooms_max",
        F.coalesce(_numeric(F.col("bedrooms_raw"), r"a\s*" + NUMBER), F.col("bedrooms_min").cast("double")).cast("int"),
    )
    .withColumn("bathrooms_min", _numeric(F.col("bathrooms_raw"), NUMBER).cast("int"))
    .withColumn(
        "bathrooms_max",
        F.coalesce(_numeric(F.col("bathrooms_raw"), r"a\s*" + NUMBER), F.col("bathrooms_min").cast("double")).cast("int"),
    )
    .withColumn("units", _numeric(F.col("units_raw"), NUMBER).cast("int"))
    # --- location / timestamps ---
    .withColumn("district_clean", F.trim(F.initcap(F.col("district"))))
    .withColumn("city_clean", F.trim(F.initcap(F.col("city"))))
    .withColumn("ingested_at_ts", F.to_timestamp("_ingested_at"))
    .withColumn("area_avg_m2", (F.col("area_min_m2") + F.col("area_max_m2")) / 2)
    .withColumn(
        "price_per_m2",
        F.when(F.col("area_avg_m2") > 0, F.col("price_amount") / F.col("area_avg_m2")),
    )
)

display(silver_df)

# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_fqn)
)
print(f"Wrote {silver_df.count()} rows to {silver_fqn}.")
