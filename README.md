# Proyecto Ames de Ingeniería de Datos I

Este repositorio es el template del proyecto de la Unidad 4 de **Ingeniería de Datos I**. Cada equipo de cuatro estudiantes debe crear su propio repositorio a partir de este template y utilizarlo para organizar el pipeline, la salida, la validación y la documentación del proyecto.

El proyecto consiste en construir en **Azure Data Factory** un proceso ETL que integra información de un archivo CSV, una base PostgreSQL en Neon y colecciones de MongoDB Atlas. El resultado requerido es `salida.csv`, con una fila por propiedad y los 81 campos establecidos en el enunciado.

## Crear el repositorio del equipo

1. Una sola persona del equipo debe abrir el [repositorio template](https://github.com/angievig/ETLIngenieriaDatos1).
2. Debe seleccionar **Use this template** y luego **Create a new repository**.
3. Debe elegir el nombre y la visibilidad indicados por la docente.
4. Debe invitar como colaboradores a los otros tres integrantes.
5. Cada integrante debe aceptar la invitación y clonar el repositorio con GitHub Desktop.

La [guía para usar el repositorio en equipo](docs/GUIA_USO_REPOSITORIO_GITHUB.docx) explica este procedimiento paso a paso.

## Información del equipo

Después de crear el repositorio, reemplacen esta sección con la información real del equipo.

- **Nombre del equipo:** `[Completar]`
- **Estado del proyecto:** `[En preparación | En desarrollo | Finalizado]`

| Integrante | Usuario de GitHub |
|---|---|
| `[Nombre completo]` | `@[usuario]` |
| `[Nombre completo]` | `@[usuario]` |
| `[Nombre completo]` | `@[usuario]` |
| `[Nombre completo]` | `@[usuario]` |

## Objetivo

Construir un pipeline ETL en Azure Data Factory que investigue e integre por `PID` las fuentes entregadas, aplique las transformaciones definidas en el enunciado y produzca una salida validable con una fila por propiedad.

El equipo debe ampliar este objetivo en el reporte y relacionarlo con las decisiones de integración, las comprobaciones realizadas y los resultados observados.

## Fuentes y tecnologías

| Componente | Uso en el proyecto |
|---|---|
| `sources/AmesProperty.csv` | Fuente de datos en formato CSV |
| PostgreSQL en Neon | Tablas relacionales del proyecto |
| MongoDB Atlas | Colecciones `garage`, `pool`, `bsmt` y `misc` |
| Azure Data Factory | Orquestación e implementación del ETL |
| Jupyter y Python | Validación de `salida.csv` |

No publiquen credenciales, tokens ni cadenas de conexión de estas plataformas.

## Salida esperada

El archivo final debe guardarse como `out/salida.csv` y cumplir las reglas del enunciado. Entre las comprobaciones principales se encuentran:

- una fila por propiedad;
- 81 campos en el orden solicitado;
- unicidad y cobertura de `PID`;
- uso consistente de camel case, snake case o kebab case, sin espacios;
- conservación de `PID`, `MS` y `QC` en mayúsculas;
- uso de `NA` y `0` como únicos marcadores permitidos para valores ausentes, según corresponda;
- aplicación y validación de las transformaciones obligatorias.

Las cifras observadas deben provenir de la ejecución del equipo. No deben copiarse conteos de referencia como si fueran resultados propios.

## Estructura del repositorio

| Ruta | Contenido esperado |
|---|---|
| `sources/` | Fuentes originales, colecciones y scripts entregados |
| `pipelines/` | Exportaciones y documentación del pipeline de ADF |
| `notebooks/` | Notebook de validación y guía de uso |
| `out/` | `salida.csv` y descripción de la salida final |
| `docs/` | Guías para el reporte, la presentación y el uso del repositorio |
| `report.md` | Reporte del proyecto |
| `README.md` | Presentación general y acceso a los entregables |

No sobrescriban, renombren ni eliminen las fuentes originales sin una justificación explícita.

## Entregables

- [Reporte del proyecto](report.md)
- [Documentación del pipeline](pipelines/pipelines.md)
- Archivo final `out/salida.csv`
- [Descripción de la salida](out/out.md)
- [Notebook de validación](notebooks/validar_salida_ames.ipynb)
- Evidencias utilizadas en el reporte y la presentación

## Guías disponibles

- [Uso del repositorio y trabajo en equipo](docs/GUIA_USO_REPOSITORIO_GITHUB.docx)
- [Preparación del reporte](docs/GUIA_PREPARACION_REPORTE.md)
- [Preparación de la presentación](docs/GUIA_PREPARACION_PRESENTACION.md)
- [Uso del notebook de validación](notebooks/GUIA_USO_VALIDACION_SALIDA.md)

## Trabajo en equipo

Para reducir conflictos durante este primer proyecto:

1. acuerden quién editará cada archivo;
2. sincronicen el repositorio antes de comenzar;
3. eviten que dos personas modifiquen simultáneamente el mismo archivo;
4. revisen los cambios en GitHub Desktop antes de guardarlos;
5. publiquen los cambios y avisen al equipo cuando terminen.

Cada integrante debe trabajar desde su propia cuenta de GitHub. Los notebooks, documentos, imágenes y otros archivos binarios requieren especial coordinación porque no se combinan fácilmente.

## Seguridad

Nunca incluyan en el repositorio:

- contraseñas o tokens;
- cadenas de conexión de Neon, MongoDB Atlas o Azure;
- archivos `.env` con valores reales;
- capturas de pantalla con información de acceso;
- rutas personales innecesarias.

Si una credencial se publica accidentalmente, debe cambiarse o revocarse de inmediato. Borrarla del archivo no garantiza que desaparezca del historial.

## Licencia

Consulte el archivo [LICENSE](LICENSE).
