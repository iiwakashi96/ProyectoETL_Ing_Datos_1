# Pipeline de Azure Data Factory

Esta carpeta contiene la exportación del proyecto de Azure Data Factory con el que se
construyó `out/salida.csv`, más la documentación de cada pieza. Los archivos JSON son la
definición exacta de las canalizaciones, el flujo de datos, los conjuntos de datos y las
conexiones; las credenciales fueron retiradas antes de publicarlos.

## Arquitectura general

El proceso tiene dos canalizaciones que se ejecutan en orden:

1. **`pl_copia_mongo`** (patrón EL, extraer y cargar): copia las cuatro colecciones de
   MongoDB Atlas a archivos CSV en el Data Lake, carpeta `ames/staging/`. No transforma
   nada; su función es dejar las fuentes documentales en un formato que el flujo de datos
   pueda unir por `PID`.
2. **`pl_ames_etl`** (patrón ETL): ejecuta el flujo de datos `df_ames`, que lee el CSV de
   propiedades, la base PostgreSQL y las cuatro copias de Mongo, los une por `PID`, aplica
   las transformaciones del enunciado y escribe `ames/salida/salida.csv`.

Las dos canalizaciones usan tres conexiones (servicios vinculados): `BlobAmes` para el
almacenamiento, `NeonConn` para PostgreSQL y `MongoConn` para MongoDB Atlas.

## Canalización 1: `pl_copia_mongo`

Cuatro actividades **Copiar datos**, una por colección, independientes entre sí:

| Actividad | Origen (conjunto de datos) | Destino (conjunto de datos) | Archivo escrito | Documentos copiados |
|---|---|---|---|---|
| `copiar_pool` | `ds_mongo_pool` | `ds_stg_pool` | `staging/pool.csv` | 13 |
| `copiar_garage` | `ds_mongo_garage` | `ds_stg_garage` | `staging/garage.csv` | 2.773 |
| `copiar_bsmt` | `ds_mongo_bsmt` | `ds_stg_bsmt` | `staging/bsmt.csv` | 2.851 |
| `copiar_misc` | `ds_mongo_misc` | `ds_stg_misc` | `staging/misc.csv` | 106 |

Decisiones de esta canalización:

- En la asignación de columnas se excluyó el campo `_id` de Mongo y, en `garage`, el campo
  `Order`, porque no forman parte de la salida.
- Los campos numéricos llegan de Mongo con el sufijo `_$numberLong`; se renombraron a su
  nombre original (`PID`, `Pool Area`, `Garage Cars`, etc.) para que el flujo los una sin
  conversiones adicionales.

## Canalización 2: `pl_ames_etl`

Una sola actividad, **`unir_y_transformar`**, de tipo Flujo de datos, que ejecuta `df_ames`.

### Flujo de datos `df_ames`

El flujo tiene 14 cajas: 6 orígenes, 5 uniones, 2 transformaciones y 1 receptor.

**Orígenes**

| Caja | Conjunto de datos | Qué lee | Columnas que aporta |
|---|---|---|---|
| `csv` | `ds_csv_ames` | `AmesProperty.csv` en el contenedor `ames` | 19 generales (PID, lote, barrio, estilo, año de construcción...) |
| `postgres` | `ds_pg_ames` | Una consulta SQL sobre la base `amesDB` de Neon | PID y 40 columnas ya transformadas (ver abajo) |
| `pool`, `garage`, `bsmt`, `misc` | `ds_stg_*` | Los CSV de `staging/` escritos por `pl_copia_mongo` | 2, 7, 11 y 2 columnas respectivamente |

En el origen `csv` el campo `PID` se declara de tipo `long` en la proyección, porque un CSV
se lee como texto y las uniones necesitan que la llave tenga el mismo tipo en ambos lados.

El origen `postgres` no lee una tabla sino una consulta (está dentro de `dataflow/df_ames.dsl`).
La consulta devuelve una fila por propiedad y resuelve en la base relacional, donde es más
natural hacerlo:

- los códigos de `MS Zoning` y de las cuatro variables de calidad (`Exter Qual`,
  `Exter Cond`, `Heating QC`, `Kitchen Qual`) mediante uniones con los catálogos
  `mszoning` y `typequality`;
- `Gr Liv Area` como la suma de `1st Flr SF`, `2nd Flr SF` y `Low Qual Fin SF`;
- `Full Bath`, `Half Bath` y `Bedroom AbvGr` como la suma por `PID` de la tabla
  `floordetail`, que tiene una fila por piso (4.182 filas para 2.930 propiedades), agregada
  antes de unir para no multiplicar propiedades;
- `Mo Sold` y `Yr Sold` extraídos de `Sale Date` de la tabla `saleproperty`;
- `Central Air` de verdadero/falso a `Y`/`N`;
- `NA` o `0` donde la base trae nulos.

**Uniones** (`unirPostgres`, `unirPool`, `unirGarage`, `unirBsmt`, `unirMisc`)

Se parte del CSV, que define el universo de 2.930 propiedades, y se le pega cada fuente
con una **unión externa izquierda por `PID`**. Así una casa sin piscina, garaje, sótano o
extras se conserva, con esas columnas vacías hasta la siguiente caja. Cada fuente tiene
como máximo una fila por `PID` (verificado en la exploración y en `pl_copia_mongo`), por lo
que ninguna unión multiplica filas: la salida de `unirMisc` sigue teniendo 2.930 filas.

**`arreglar`** (Columna derivada, 29 fórmulas)

- Rellena las columnas que vienen de Mongo cuando la casa no tiene documento: `NA` en las
  11 cualitativas (`Pool QC`, `Garage Type`, `Bsmt Qual`, `Misc Feature`...) y `0` en las
  11 numéricas (`Pool Area`, `Garage Cars`, `Total Bsmt SF`, `Misc Val`...), con `iifNull`.
- `Year Remod/Add` vacío toma el valor de `Year Built`.
- `Lot Frontage` vacío se pone en `0` (único campo numérico del CSV con vacíos, 490 casas).
- `Sale Type` se recorta con `trim`, porque la base trae `WD ` con un espacio final.
- Transformaciones opcionales: `Neighborhood`, `Condition 1`, `Condition 2` y `Lot Shape`
  pasan de descripción a código con expresiones `case` (por ejemplo `North Ames` → `Names`,
  `Slightly irregular` → `IR1`).

**`ordenar`** (Seleccionar)

Deja un solo `PID` (el del CSV, descartando los cinco repetidos que dejan las uniones),
ordena las 81 columnas como en el enunciado y las renombra a **snake_case** sin espacios,
conservando `PID`, `MS` y `QC` en mayúsculas (`MS_sub_class`, `heating_QC`, `year_remod_add`,
`1st_flr_sf`, `sale_price`).

**`salida`** (Receptor)

Escribe un único archivo `salida.csv` (`ds_salida`: contenedor `ames`, carpeta `salida`,
separador `;`, UTF-8, encabezado en la primera fila). Se fuerza una sola partición para
que no se generen varios archivos parciales.

## Ejecución

- Ambas canalizaciones se ejecutaron con **Depurar** desde Data Factory Studio, con la
  depuración de flujo de datos activa. `pl_copia_mongo` el 16 de noviembre de 2026 y
  `pl_ames_etl` el 19 de noviembre de 2026 (última versión).
- Resultado de la última ejecución: 2.930 filas leídas en `csv` y `postgres`, 13, 2.773,
  2.851 y 106 en las copias de Mongo, 2.930 filas escritas en `salida`.
- La salida fue validada con el notebook del curso (`notebooks/validar_salida_ames.ipynb`)
  con `NAMING_STYLE = "snake_case"`: 7 criterios aprobados.

## Contenido de esta carpeta

| Ruta | Qué es |
|---|---|
| `pipeline/pl_copia_mongo.json` | Definición de la canalización 1 (4 actividades de copia con sus asignaciones de columnas) |
| `pipeline/pl_ames_etl.json` | Definición de la canalización 2 (actividad de flujo de datos) |
| `dataflow/df_ames.json` | Definición del flujo de datos en formato JSON (orígenes, transformaciones, receptor) |
| `dataflow/df_ames.dsl` | El mismo flujo en el lenguaje de script de Data Factory; aquí se leen las expresiones completas y la consulta SQL de origen |
| `dataset/ds_csv_ames.json` | Conjunto de datos del CSV de propiedades |
| `dataset/ds_pg_ames.json` | Conjunto de datos de PostgreSQL |
| `dataset/ds_mongo_*.json` | Conjuntos de datos de las 4 colecciones de Mongo (origen de la copia) |
| `dataset/ds_stg_*.json` | Conjuntos de datos de las 4 copias en `staging/` (destino de la copia y origen del flujo) |
| `dataset/ds_salida.json` | Conjunto de datos del archivo de salida |
| `linkedService/BlobAmes.json` | Conexión al almacenamiento de Azure (sin clave de cuenta) |
| `linkedService/NeonConn.json` | Conexión a PostgreSQL en Neon (servidor, usuario y contraseña retirados) |
| `linkedService/MongoConn.json` | Conexión a MongoDB Atlas (cadena de conexión retirada) |
| `arm_template/` | Plantilla ARM exportada desde Administrar → Plantilla de ARM: permite recrear la fábrica completa en otra suscripción; las conexiones quedan como parámetros vacíos |

## Cómo reproducirlo

1. Crear un Data Factory e importar `arm_template/ARMTemplateForFactory.json`
   (Administrar → Plantilla de ARM → Importar), indicando las tres cadenas de conexión
   como parámetros.
2. Subir `sources/AmesProperty.csv` al contenedor `ames` del almacenamiento.
3. Ejecutar `pl_copia_mongo` y después `pl_ames_etl`.
4. Descargar `ames/salida/salida.csv` y validarlo con el notebook de `notebooks/`.

## Seguridad

Ningún archivo de esta carpeta contiene contraseñas, claves de cuenta, credenciales
cifradas ni cadenas de conexión completas. Los valores retirados aparecen como
`<servidor>`, `<usuario>` y `<contraseña>`.
