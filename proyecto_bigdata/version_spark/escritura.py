"""
version_spark/escritura.py
==========================
Persistencia en Parquet (formato columnar, comprimido con snappy).

Dos salidas:

1. Resultados de las 4 consultas -> `resultados/spark/<consulta>/`
   Son tablas pequeñas (cientos o miles de filas), así que se reducen a 1 partición
   con `coalesce(1)` y quedan en un único archivo part-*.parquet por consulta.

2. (Opcional) Base limpia y enriquecida -> `resultados/spark/ventas_limpias/anio=AAAA/mes=M/`
   Particionada por año y mes (`partitionBy`). Una consulta posterior que filtre por
   fecha, p. ej. `where(anio == 2025)`, solo lee las carpetas de ese año (*partition
   pruning*), y por ser columnar solo lee las columnas que usa (*column pruning*).
   Es la capa "lista para análisis" que evitaría volver a parsear y limpiar los CSV.

Spark escribe en paralelo (un archivo por partición/tarea) y de forma atómica: primero
en una carpeta _temporary y al final la renombra, dejando un marcador _SUCCESS.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame

RAIZ = Path(__file__).resolve().parents[1]
DIR_RESULTADOS_SPARK = RAIZ / "resultados" / "spark"


def guardar_resultado(df: DataFrame, nombre: str, directorio: Path = DIR_RESULTADOS_SPARK) -> Path:
    """Escribe un resultado agregado (pequeño) como un único archivo Parquet."""
    ruta = directorio / nombre
    df.coalesce(1).write.mode("overwrite").parquet(str(ruta))
    return ruta


def guardar_base_particionada(
    base: DataFrame, directorio: Path = DIR_RESULTADOS_SPARK / "ventas_limpias"
) -> Path:
    """Escribe la base limpia particionada por año y mes.

    `repartition("anio", "mes")` agrupa antes las filas de cada mes en la misma tarea,
    para que cada carpeta tenga pocos archivos grandes en vez de cientos de archivos
    pequeños (problema clásico de *small files*).
    """
    (base.repartition("anio", "mes")
         .write.mode("overwrite")
         .partitionBy("anio", "mes")
         .parquet(str(directorio)))
    return directorio
