"""
version_spark/joins.py
======================
Cruces del modelo en estrella, eligiendo la estrategia de join según el tamaño de cada
dimensión (la razón por la que el dataset tiene dimensiones de tamaños tan distintos):

| Dimensión | Filas  | Estrategia                 | Por qué                                         |
|-----------|--------|----------------------------|-------------------------------------------------|
| productos | 60 000 | broadcast hash join (INNER)| ~5 MB: se copia una vez a cada tarea y la tabla  |
|           |        |                            | de hechos NO se mueve (sin shuffle)             |
| tiendas   | 600    | broadcast hash join (INNER)| ídem, unos KB                                   |
| clientes  | 5 M    | sort-merge join (LEFT)     | demasiado grande para copiarla a cada tarea en  |
|           |        |                            | un equipo de 8 GB: ambas tablas se reparten por |
|           |        |                            | hash de cliente_id (shuffle) y se cruzan        |

Optimización para clientes: la única consulta que necesita datos del cliente es la 4
(segmento). Por eso el join con clientes NO se hace sobre la base común (que se guarda
en caché para las consultas 1-3), sino solo dentro de la consulta 4 y solo con las
columnas que esta usa (cliente_id, anio_mes, venta_neta). Así el shuffle mueve 3
columnas en vez de ~20.

(Se descartó agregar por (mes, cliente) antes del join: con ~24 compras por cliente en
36 meses casi cada par mes-cliente tiene una sola fila, de modo que la reducción es
mínima y se añadiría un segundo shuffle.)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

COLUMNAS_PRODUCTO = ["producto_id", "categoria", "subcategoria", "costo_unitario"]
COLUMNAS_TIENDA = ["tienda_id", "region", "departamento", "ciudad", "tipo_tienda"]
COLUMNAS_CLIENTE = ["cliente_id", "segmento"]


def unir_productos(tx: DataFrame, productos: DataFrame) -> DataFrame:
    """INNER JOIN con productos usando broadcast explícito."""
    return tx.join(F.broadcast(productos.select(*COLUMNAS_PRODUCTO)), on="producto_id", how="inner")


def unir_tiendas(tx: DataFrame, tiendas: DataFrame) -> DataFrame:
    """INNER JOIN con tiendas usando broadcast explícito.

    `departamento` y `region` son los de la TIENDA (donde ocurre la venta), igual que en pandas.
    """
    return tx.join(F.broadcast(tiendas.select(*COLUMNAS_TIENDA)), on="tienda_id", how="inner")


def unir_clientes(df: DataFrame, clientes: DataFrame) -> DataFrame:
    """LEFT JOIN con clientes (sort-merge con shuffle). Conserva el 8 % de compras anónimas."""
    return df.join(clientes.select(*COLUMNAS_CLIENTE), on="cliente_id", how="left")


def unir_modelo_estrella(tx_limpio: DataFrame, dimensiones: dict[str, DataFrame]) -> DataFrame:
    """Hechos + productos + tiendas (los dos broadcast). Base de las consultas 1, 2 y 3."""
    return unir_tiendas(unir_productos(tx_limpio, dimensiones["productos"]), dimensiones["tiendas"])
