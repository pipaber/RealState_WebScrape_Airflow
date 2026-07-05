# Databricks notebook source
# MAGIC %md
# MAGIC # Configuración: parámetros del pipeline
# MAGIC Única fuente de verdad para el pipeline medallion. Se carga en cada
# MAGIC notebook de capa con `%run ./config/parameters`.
# MAGIC
# MAGIC `source_path` se descubre automáticamente: recorre las particiones
# MAGIC estilo Hive bajo `raw/airflow/G1/source=urbania/` y elige el archivo
# MAGIC `.jsonl` modificado más recientemente, para no tener que escribir a
# MAGIC mano un run_id o ingest_date.
# MAGIC
# MAGIC Las tablas viven en los esquemas compartidos `bronze` / `silver` / `gold`
# MAGIC del catálogo `g101_catalog`; `prefix` se usa como prefijo del nombre de
# MAGIC tabla para no chocar con las de otros estudiantes.

# COMMAND ----------

BASE_RAW_PATH = "abfss://datalake@stdemdsai.dfs.core.windows.net/raw/airflow/G1/source=urbania/"


def _find_latest_jsonl(path: str):
    """Recorre recursivamente las particiones estilo Hive y devuelve el FileInfo del .jsonl más reciente."""
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
    raise FileNotFoundError(f"No se encontraron archivos .jsonl bajo {BASE_RAW_PATH}")

prefix = 'pipaber'

params = {
    'catalog': 'g101_catalog',
    'prefix': prefix,
    'source_path': _latest_file.path,
    'bronze_schema': 'bronze',
    'silver_schema': 'silver',
    'gold_schema': 'gold',
    'bronze_table': f'{prefix}_urbania',
    'silver_table': f'{prefix}_urbania',
    'gold_table': f'{prefix}_urbania',
}
print('Diccionario importado: params')
