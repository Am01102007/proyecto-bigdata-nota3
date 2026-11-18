"""
version_spark/cargador.py
=========================
Lectura de la tabla de hechos y de las dimensiones como DataFrames de Spark.

Diferencias clave frente a `version_pandas/cargador.py`:

- **Nada se lee todavía.** `spark.read.csv` solo construye el plan (evaluación perezosa).
  Los datos se leen cuando una acción (count, write, collect) lo exige, y entonces cada
  núcleo lee en paralelo un trozo de archivo (por defecto bloques de 128 MB).
- **Esquema explícito** (`comun/esquemas.py`): evita `inferSchema=True`, que obliga a
  recorrer todo el CSV una vez más solo para adivinar los tipos.
- **No hacen falta chunks ni `usecols`:** Spark procesa por particiones y, gracias a la
  poda de columnas del optimizador Catalyst, solo materializa las columnas que el plan usa.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from comun.esquemas import (  # noqa: E402
    esquema_spark_clientes,
    esquema_spark_productos,
    esquema_spark_tiendas,
    esquema_spark_transacciones,
)
from comun.muestras import DIR_DIMENSIONES, rutas_muestra  # noqa: E402

# Opciones comunes de lectura CSV (los archivos los escribe PyArrow con comillas dobles).
OPCIONES_CSV = {
    "header": "true",
    "quote": '"',
    "escape": '"',
    "encoding": "UTF-8",
    "mode": "PERMISSIVE",     # una fila malformada se convierte en nulos, no detiene el trabajo
    "dateFormat": "yyyy-MM-dd",
}


def cargar_transacciones(spark: SparkSession, porcentaje: float) -> DataFrame:
    """Tabla de hechos para la muestra del `porcentaje` % (mismos archivos que pandas)."""
    rutas = [str(p) for p in rutas_muestra(porcentaje)]
    return spark.read.options(**OPCIONES_CSV).schema(esquema_spark_transacciones()).csv(rutas)


def cargar_dimensiones(
    spark: SparkSession, dir_dimensiones: Path = DIR_DIMENSIONES
) -> dict[str, DataFrame]:
    """Maestros de productos, tiendas y clientes."""
    faltantes = [n for n in ("productos", "tiendas", "clientes")
                 if not (dir_dimensiones / f"{n}.csv").exists()]
    if faltantes:
        raise FileNotFoundError(
            f"Faltan {faltantes} en {dir_dimensiones}. Ejecute primero scripts/generar_datos.py"
        )

    def leer(nombre: str, esquema) -> DataFrame:
        return spark.read.options(**OPCIONES_CSV).schema(esquema).csv(str(dir_dimensiones / f"{nombre}.csv"))

    return {
        "productos": leer("productos", esquema_spark_productos()),
        "tiendas": leer("tiendas", esquema_spark_tiendas()),
        "clientes": leer("clientes", esquema_spark_clientes()),
    }
