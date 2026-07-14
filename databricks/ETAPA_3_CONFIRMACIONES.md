# ETAPA 3: Ingesta y almacenamiento

## Estado de avance

| Paso | Resultado | Estado |
|---|---|---|
| Ampliar el scraping a 1-4 dormitorios | Cuatro tareas mapeadas de Airflow | Confirmado |
| Generar artefactos raw inmutables | NDJSON y manifiesto por segmento | Confirmado |
| Validar ejecución de Airflow | Cuatro manifests `success` | Confirmado |
| Ingestar anuncios en Bronze | Auto Loader + Delta `listings_raw` | Confirmado |
| Ingestar manifests en Bronze | Auto Loader + Delta `ingestion_runs` | Confirmado |

## Ejecución validada: 2026-07-13

| Dormitorios | Registros del manifest | Registros del NDJSON | Estado |
|---:|---:|---:|---|
| 1 | 846 | 846 | `success` |
| 2 | 1,181 | 1,181 | `success` |
| 3 | 1,115 | 1,115 | `success` |
| 4 | 187 | 187 | `success` |

Total de observaciones raw: **3,329**.

Cada segmento produjo:

```text
raw/airflow/G1/source=urbania/operation=alquiler/property=departamento/
bedrooms=<1|2|3|4>/ingest_date=2026-07-13/
├── listings_<run_id>.jsonl
└── manifest_<run_id>.json
```

## Reglas confirmadas de almacenamiento raw

- Cada tarea de Airflow procesa un único segmento de dormitorios.
- El `run_id` es estable ante reintentos de la misma ejecución de Airflow.
- Se suben exclusivamente el NDJSON y el manifiesto del run.
- Un blob existente se omite; no se sobrescribe el raw publicado.
- La carga hacia Databricks usará checkpoints independientes por tipo de archivo.

## Validación de Bronze: 2026-07-14

El notebook `01_bronze_ingest.py` terminó correctamente y creó las dos tablas
Delta del Data Product:

| Tabla | Resultado validado |
|---|---|
| `g101_cmc_mercado_alquiler.bronze.listings_raw` | 9,442 observaciones históricas de Raw |
| `g101_cmc_mercado_alquiler.bronze.ingestion_runs` | 4 manifests `success` de la ejecución más reciente |

Distribución histórica de `listings_raw` mostrada por el notebook:

| Dormitorios | Observaciones raw | Runs |
|---:|---:|---:|
| 1 | 1,725 | 2 |
| 2 | 5,129 | 5 |
| 3 | 2,228 | 2 |
| 4 | 361 | 2 |

La diferencia entre las observaciones históricas y los cuatro manifests
mostrados corresponde a ejecuciones anteriores que se preservan
intencionalmente en Bronze. La deduplicación de archivos/reintentos se controla
con checkpoint y `MERGE`; los cambios entre runs se conservan como historial.

## Siguiente paso

Construir Silver: normalizar los atributos, aplicar reglas de calidad y separar
los registros rechazados en una tabla de cuarentena dentro del schema `silver`.
