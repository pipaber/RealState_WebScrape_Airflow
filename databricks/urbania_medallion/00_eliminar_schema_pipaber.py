# Databricks notebook source
# MAGIC %md
# MAGIC # Limpieza: eliminar el esquema `pipaber` creado por error
# MAGIC Las primeras corridas crearon un esquema `pipaber` dentro de
# MAGIC `g101_catalog` con las tablas bronze/silver/gold adentro, en vez de usar
# MAGIC los esquemas compartidos `bronze`, `silver` y `gold`. Este notebook
# MAGIC elimina ese esquema (y todas las tablas que contiene) antes de volver a
# MAGIC cargar los datos con la estructura correcta.
# MAGIC
# MAGIC Ejecutar una sola vez, antes de correr `01_bronze`.

# COMMAND ----------

CATALOG = "g101_catalog"
ESQUEMA_A_BORRAR = "pipaber"
esquema_fqn = f"{CATALOG}.{ESQUEMA_A_BORRAR}"

print(f"Se eliminará el esquema: {esquema_fqn} (y todas sus tablas)")
spark.sql(f"DROP SCHEMA IF EXISTS `{CATALOG}`.`{ESQUEMA_A_BORRAR}` CASCADE")
print(f"Esquema {esquema_fqn} eliminado (si existía).")
