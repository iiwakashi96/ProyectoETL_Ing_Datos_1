# Salida final del ETL: `salida.csv`

Archivo producido por la canalización `pl_ames_etl` de Azure Data Factory (flujo de datos
`df_ames`) el 19 de noviembre de 2026. Es la versión final entregada; se descargó sin
modificaciones desde el contenedor `ames`, carpeta `salida`, del almacenamiento del proyecto.

## Características

| Aspecto | Valor |
|---|---|
| Filas | 2.930, una por propiedad, más la fila de encabezado |
| Columnas | 81, en el orden definido en el enunciado |
| Llave | `PID`, numérica, sin repetidos ni vacíos; cubre exactamente las 2.930 propiedades de `sources/AmesProperty.csv` |
| Separador | `;` |
| Codificación | UTF-8, sin marca de orden de bytes |
| Encabezados | snake_case, sin espacios; `PID`, `MS` y `QC` se conservan en mayúsculas (`MS_sub_class`, `MS_zoning`, `heating_QC`, `pool_QC`) |
| Ausencias | `NA` en campos cualitativos y `0` en campos numéricos; no hay celdas vacías ni otros marcadores |
| Tamaño | 940 KB |

## Origen de las columnas

| Fuente | Columnas que aporta |
|---|---|
| `AmesProperty.csv` | 19: `PID`, `lot_frontage`, `lot_area`, `street`, `alley`, `lot_shape`, `land_contour`, `utilities`, `lot_config`, `land_slope`, `neighborhood`, `condition_1`, `condition_2`, `bldg_type`, `house_style`, `overall_qual`, `overall_cond`, `year_built`, `year_remod_add` |
| PostgreSQL (`amesdbtemp`, `saleproperty`, `floordetail` y catálogos) | 40: desde `MS_sub_class` hasta `sale_price`, incluidas las calculadas `gr_liv_area`, `full_bath`, `half_bath`, `bedroom`, `mo_sold`, `yr_sold` |
| MongoDB `garage` | 7: `garage_type`, `garage_yr_blt`, `garage_finish`, `garage_cars`, `garage_area`, `garage_qual`, `garage_cond` |
| MongoDB `bsmt` | 11: `bsmt_qual`, `bsmt_cond`, `bsmt_exposure`, `bsmt_fin_type_1`, `bsmt_fin_sf_1`, `bsmt_fin_type_2`, `bsmt_fin_sf_2`, `bsmt_unf_sf`, `total_bsmt_sf`, `bsmt_full_bath`, `bsmt_half_bath` |
| MongoDB `pool` | 2: `pool_area`, `pool_QC` |
| MongoDB `misc` | 2: `misc_feature`, `misc_val` |

## Transformaciones aplicadas

- `gr_liv_area` = `1st_flr_sf` + `2nd_flr_sf` + `low_qual_fin_sf`.
- `bedroom`, `full_bath` y `half_bath`: suma por propiedad de los pisos de `floordetail`.
- `mo_sold` y `yr_sold`: mes y año de la fecha de venta de `saleproperty`.
- `year_remod_add`: cuando venía vacío se tomó `year_built`.
- `MS_zoning`, `exter_qual`, `exter_cond`, `heating_QC`, `kitchen_qual`: códigos tomados de
  los catálogos `mszoning` y `typequality`.
- `central_air`: de verdadero/falso a `Y`/`N`.
- Propiedades sin documento en `garage`, `bsmt`, `pool` o `misc`: `NA` en sus columnas
  cualitativas y `0` en las numéricas.
- `lot_frontage` vacío en la fuente (490 propiedades): `0`.
- `sale_type`: se eliminó el espacio final que traía el valor `WD` en la base.
- Opcionales aplicadas: `neighborhood`, `condition_1`, `condition_2` y `lot_shape` en
  códigos (`North Ames` → `Names`, `Normal` → `Norm`, `Regular` → `Reg`, etc.).

## Validación

Validado con `notebooks/validar_salida_ames.ipynb` (`NAMING_STYLE = "snake_case"`,
comparación con fuentes activada) sobre esta misma versión del archivo: los siete criterios
de la rúbrica (dimensiones, ausencia de nulos, nombres de columnas, transformaciones
obligatorias, transformaciones opcionales, variables de calidad y extracción de MongoDB)
quedaron en `APROBADO`. El detalle de las pruebas está en el notebook y en la sección de
validación de `report.md`.

Versiones anteriores del archivo (18 de noviembre) conservaban los nombres de columna con
espacios del archivo de ejemplo y el código `NAmes`; se reemplazaron por esta versión tras
ajustar el flujo a la convención exigida por el notebook del curso.
