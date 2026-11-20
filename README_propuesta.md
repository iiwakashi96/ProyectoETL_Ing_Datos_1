# Proyecto ETL Ames — Ingeniería de Datos I

Repositorio del equipo para el proyecto de la Unidad 4 de **Ingeniería de Datos I**
(Maestría en Ciencia de Datos, Universidad Icesi, periodo 2026-2).

El proyecto construye en **Azure Data Factory** un proceso ETL que integra tres fuentes
sobre las 2.930 propiedades del conjunto de datos Ames: un archivo CSV, una base de datos
PostgreSQL en Neon y cuatro colecciones de MongoDB Atlas. El resultado es `out/salida.csv`,
con una fila por propiedad y los 81 campos definidos en el enunciado.

## Información del equipo

- **Nombre del equipo:** Grupo 7
- **Estado del proyecto:** En desarrollo. Pipeline construido y ejecutado; `salida.csv`
  validado el 19 de noviembre de 2026; reporte y presentación en elaboración.

| Integrante | Usuario de GitHub |
|---|---|
| Mateo Sanchez | [@mateosanchez888](https://github.com/mateosanchez888) |
| Luisa Mosquera | [@luisamosquera96](https://github.com/luisamosquera96) |
| Dario Yepez | [@dfyepezv](https://github.com/dfyepezv) |
| Juan Pablo Jimenez | [@iiwakashi96](https://github.com/iiwakashi96) |
| Nathalia Herrera | [@nathaherrera](https://github.com/nathaherrera) |

## Objetivo

Construir un pipeline ETL en Azure Data Factory que extraiga las fuentes entregadas, las
integre por `PID` conservando las 2.930 propiedades, aplique las transformaciones del
enunciado (área habitable, agregación de habitaciones y baños por piso, mes y año de
venta, año de remodelación, códigos de calidad y zonificación, marcadores `NA` y `0` para
ausencias) y produzca `salida.csv` con los 81 campos en el orden requerido, con una
convención de nombres sin espacios.

## Fuentes y tecnologías

| Componente | Uso en el proyecto |
|---|---|
| `sources/AmesProperty.csv` | 19 campos generales de cada propiedad (2.930 filas, separador `;`). Define el universo de `PID` |
| PostgreSQL en Neon (`amesDB`) | 6 tablas: `amesdbtemp` (35 columnas), `saleproperty` (fecha y precio de venta), `floordetail` (habitaciones y baños por piso, 4.182 filas) y los catálogos `mszoning`, `mssubclass` y `typequality` |
| MongoDB Atlas (`test`) | Colecciones `garage` (2.773 documentos), `bsmt` (2.851), `misc` (106) y `pool` (13) |
| Azure Blob Storage (Data Lake) | Contenedor `ames`: CSV de entrada, copias de Mongo en `staging/` y la salida en `salida/` |
| Azure Data Factory | Dos canalizaciones: `pl_copia_mongo` (copia las colecciones a CSV) y `pl_ames_etl` (flujo de datos `df_ames` que une y transforma) |
| pgAdmin y MongoDB Compass | Exploración de las fuentes y pruebas de la consulta de origen |
| Python 3 con pandas y Jupyter | Validación de `salida.csv` con el notebook del curso |
| GitHub y GitHub Desktop | Control de versiones y entrega |

## Convención de la salida

- 81 columnas en el orden del enunciado, nombres en **snake_case** sin espacios, con las
  siglas `PID`, `MS` y `QC` en mayúsculas (por ejemplo `MS_sub_class`, `heating_QC`,
  `year_remod_add`).
- Separador `;`, codificación UTF-8, primera fila como encabezado.
- Valores ausentes: `NA` en campos cualitativos y `0` en campos numéricos.
- Transformaciones opcionales aplicadas: `neighborhood`, `condition_1`, `condition_2` y
  `lot_shape` en códigos.

## Estructura del repositorio

| Ruta | Contenido |
|---|---|
| `sources/` | Fuentes originales entregadas por el curso (no se modifican) |
| `pipelines/` | JSON exportados de Data Factory (canalizaciones, flujo de datos, conjuntos de datos y servicios vinculados sin credenciales), plantilla ARM, capturas y `pipelines.md` |
| `notebooks/` | Notebook de validación `validar_salida_ames.ipynb` ejecutado sobre nuestra salida, y su guía |
| `out/` | `salida.csv` final y `out.md` con su descripción |
| `docs/` | Guías del curso para el reporte, la presentación y el uso del repositorio |
| `report.md` | Reporte del proyecto |
| `README.md` | Este archivo |

## Entregables

- [Reporte del proyecto](report.md)
- [Documentación del pipeline](pipelines/pipelines.md)
- [Archivo final `salida.csv`](out/salida.csv) y [descripción de la salida](out/out.md)
- [Notebook de validación](notebooks/validar_salida_ames.ipynb)

## Seguridad

Este repositorio no contiene contraseñas, tokens ni cadenas de conexión de Neon, MongoDB
Atlas o Azure. Los servicios vinculados exportados en `pipelines/` tienen las credenciales
eliminadas.

## Licencia

Consulte el archivo [LICENSE](LICENSE).
