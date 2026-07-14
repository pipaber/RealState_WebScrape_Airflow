# ETAPA 2: Infraestructura y configuración base

Este documento registra las comprobaciones realizadas para el Data Product `cmc_mercado_alquiler`.

## Estado de avance

| Paso | Resultado | Estado |
|---|---|---|
| Solicitar el Data Product mediante Data Platform | Solicitud procesada | Confirmado |
| Crear un catálogo por Data Product | `g101_cmc_mercado_alquiler` | Confirmado |
| Crear esquemas Medallion | `bronze`, `silver`, `gold` | Confirmado |
| Configurar accesos por grupos | Grupos gestionados por Data Platform | Confirmado |
| Verificar acceso administrativo | `ALL PRIVILEGES` efectivo | Confirmado |
| Verificar escritura Delta | Creación, inserción, lectura y eliminación de tabla de prueba | Confirmado |

## 1. Flujo de creación utilizado

El catálogo no se creó manualmente. Se utilizó el flujo gobernado del repositorio Data Platform del curso:

```text
data_product_request/submit_request.py
```

Solicitud presentada:

```python
group = "g101"
leader = "piero.palacios@utec.edu.pe"
members = ""

data_products = [
    ("cmc", "comercial", "mercado_alquiler"),
]
```

La plataforma validó la solicitud, creó los objetos de Unity Catalog y configuró los grupos. Este mecanismo reemplaza la creación manual con `CREATE CATALOG`.

## 2. Catálogo confirmado

```text
g101_cmc_mercado_alquiler
```

El catálogo corresponde exclusivamente al Data Product:

```text
cmc_mercado_alquiler
```

## 3. Esquemas confirmados

```text
g101_cmc_mercado_alquiler.bronze
g101_cmc_mercado_alquiler.silver
g101_cmc_mercado_alquiler.gold
```

Unity Catalog también creó automáticamente:

```text
g101_cmc_mercado_alquiler.default
g101_cmc_mercado_alquiler.information_schema
```

Estos dos esquemas automáticos no deben eliminarse ni utilizarse para las tablas del pipeline.

La plataforma del curso solo crea los tres esquemas Medallion. En consecuencia, no se creará un esquema `support`. Las tablas auxiliares serán:

```text
g101_cmc_mercado_alquiler.silver.listings_quarantine
g101_cmc_mercado_alquiler.silver.data_quality_results
```

## 4. Modelo de acceso confirmado

Grupos administrados por la plataforma:

```text
g101_cmc_mercado_alquiler_admin
g101_cmc_mercado_alquiler_writer
g101_cmc_mercado_alquiler_reader
```

Accesos efectivos observados para el usuario del grupo:

| Principal | Acceso observado |
|---|---|
| `g101_cmc_mercado_alquiler_admin` | `ALL PRIVILEGES` |
| `g101_cmc_mercado_alquiler_reader` | `BROWSE`, `EXECUTE`, `READ VOLUME`, `SELECT`, `USE CATALOG`, `USE SCHEMA` |

El usuario validado es:

```text
piero.palacios@utec.edu.pe
```

`ALL PRIVILEGES` no incluye necesariamente `MANAGE`. Por ello, el usuario puede desarrollar y operar las tablas del Data Product, pero la administración completa de grants permanece gobernada por la Data Platform y el profesor.

## 5. Prueba de escritura confirmada

Se verificaron correctamente las siguientes operaciones en Bronze:

1. crear una tabla Delta;
2. insertar un registro;
3. consultar el registro;
4. eliminar la tabla de prueba.

La prueba confirma que el usuario puede crear y modificar las tablas requeridas por el pipeline.

## 6. Reglas operativas

- No ejecutar `DROP CATALOG`.
- No ejecutar `DROP SCHEMA ... CASCADE`.
- No modificar los grupos creados por Data Platform.
- No crear un catálogo alternativo para el mismo Data Product.
- No utilizar `default` para las tablas del pipeline.
- Mantener los objetos del producto dentro de `bronze`, `silver` y `gold`.
- Usar scripts idempotentes con `IF NOT EXISTS`, `MERGE` o reemplazo controlado.

## Cierre de la ETAPA 2

La infraestructura mínima requerida está disponible y validada:

```text
Catálogo gobernado + esquemas Medallion + grupos + escritura Delta
```

El siguiente bloque de trabajo será la ETAPA 3: adaptar el scraping para 1 a 4 dormitorios e implementar la ingesta incremental hacia Bronze.
