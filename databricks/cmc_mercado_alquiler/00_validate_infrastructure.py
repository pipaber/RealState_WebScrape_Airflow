# Databricks notebook source
# MAGIC %md
# MAGIC # Validación de infraestructura del Data Product
# MAGIC
# MAGIC Comprueba de forma no destructiva que el catálogo, los esquemas Medallion
# MAGIC y los accesos del usuario están disponibles. Este notebook no crea ni
# MAGIC elimina catálogos, esquemas, grupos o tablas.

# COMMAND ----------

CATALOG = "g101_cmc_mercado_alquiler"
REQUIRED_SCHEMAS = {"bronze", "silver", "gold"}

ADMIN_GROUP = f"{CATALOG}_admin"
WRITER_GROUP = f"{CATALOG}_writer"
READER_GROUP = f"{CATALOG}_reader"

# COMMAND ----------

current_user = spark.sql("SELECT current_user()").first()[0]

membership = spark.sql(
    f"""
    SELECT
      is_account_group_member('{ADMIN_GROUP}') AS is_admin,
      is_account_group_member('{WRITER_GROUP}') AS is_writer,
      is_account_group_member('{READER_GROUP}') AS is_reader
    """
).first().asDict()

print(f"Usuario actual: {current_user}")
print(f"Membresías efectivas: {membership}")

if not (membership["is_admin"] or membership["is_writer"]):
    raise PermissionError(
        f"{current_user} debe pertenecer a {ADMIN_GROUP} o {WRITER_GROUP} "
        "para ejecutar el pipeline."
    )

# COMMAND ----------

schema_rows = spark.sql(f"SHOW SCHEMAS IN `{CATALOG}`").collect()
existing_schemas = {row[0].lower() for row in schema_rows}
missing_schemas = REQUIRED_SCHEMAS - existing_schemas

print(f"Esquemas encontrados: {sorted(existing_schemas)}")

if missing_schemas:
    raise RuntimeError(
        f"La Data Platform todavía no creó los esquemas requeridos: "
        f"{sorted(missing_schemas)}"
    )

# COMMAND ----------

grants_df = spark.sql(f"SHOW GRANTS ON CATALOG `{CATALOG}`")
display(grants_df)

# COMMAND ----------

print(
    "Validación completada: catálogo, esquemas Medallion y acceso de escritura "
    "están disponibles."
)
