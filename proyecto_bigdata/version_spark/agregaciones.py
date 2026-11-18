"""
version_spark/agregaciones.py
=============================
Las 4 consultas de negocio. Misma lógica y mismas columnas de salida que
`version_pandas/agregaciones.py`, para poder validar la equivalencia.

Se usan a propósito tres estilos de Spark, todos compilados por el mismo optimizador
(Catalyst) al mismo plan físico:

  - Consulta 1: API de DataFrames (groupBy + agg).
  - Consulta 2: Spark SQL sobre una vista temporal (conteos condicionales en una pasada).
  - Consulta 3: función de ventana `rank()` particionada por departamento.
  - Consulta 4: LEFT JOIN con clientes + `countDistinct` (agregación en dos fases).

Por qué funciona donde pandas por chunks no:
  - `groupBy` hace una agregación parcial en cada partición y luego redistribuye por
    clave (shuffle) para combinarla: con sumas es igual que los chunks, pero...
  - `countDistinct` y `rank` necesitan ver TODAS las filas de cada grupo. El shuffle
    garantiza que todas las filas de una clave lleguen a la misma tarea, y si no caben
    en memoria Spark las ordena y vuelca a disco (spill) en vez de fallar.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

SIN_FIDELIZACION = "Sin fidelización"

# Columnas que necesitan las 4 consultas. La base se guarda en caché con SOLO estas
# columnas (proyección): menos memoria y menos disco si la caché se derrama.
COLUMNAS_BASE = [
    "anio_mes", "anio", "mes", "region", "departamento", "categoria", "canal", "estado",
    "producto_id", "cliente_id", "cantidad", "venta_neta", "margen_bruto",
]


def calcular_metricas_financieras(df: DataFrame) -> DataFrame:
    """precio_cobrado = precio * (1 - descuento); venta_neta = cantidad * precio_cobrado;
    margen_bruto = venta_neta - cantidad * costo_unitario. (Mismas fórmulas que pandas.)"""
    precio_cobrado = F.col("precio_unitario") * (1 - F.col("descuento_pct") / 100.0)
    venta_neta = F.col("cantidad") * precio_cobrado
    return (
        df.withColumn("venta_neta", venta_neta)
        .withColumn("margen_bruto", venta_neta - F.col("cantidad") * F.col("costo_unitario"))
    )


def preparar_base(df_enriquecido: DataFrame) -> DataFrame:
    """Base analítica común: métricas financieras + solo las columnas necesarias."""
    return calcular_metricas_financieras(df_enriquecido).select(*COLUMNAS_BASE)


# ---------------------------------------------------------------------------
# Consulta 1 - API de DataFrames
# ---------------------------------------------------------------------------
def consulta_1_ventas_margen(base: DataFrame) -> DataFrame:
    """Ventas netas, unidades y margen bruto por mes, región y categoría (sin canceladas)."""
    return (
        base.where(F.col("estado") != "CANCELADA")
        .groupBy("anio_mes", "region", "categoria")
        .agg(
            F.sum("cantidad").cast("long").alias("unidades_vendidas"),
            F.sum("venta_neta").alias("venta_neta_total"),
            F.sum("margen_bruto").alias("margen_bruto_total"),
        )
        .withColumn(
            "margen_pct",
            F.coalesce(F.round(F.col("margen_bruto_total") / F.col("venta_neta_total") * 100, 2),
                       F.lit(0.0)),
        )
        .orderBy("anio_mes", "region", "categoria")
    )


# ---------------------------------------------------------------------------
# Consulta 2 - Spark SQL
# ---------------------------------------------------------------------------
SQL_CONSULTA_2 = """
SELECT canal,
       categoria,
       COUNT(*)                                          AS total_transacciones,
       SUM(CASE WHEN estado = 'DEVUELTA'  THEN 1 ELSE 0 END) AS transacciones_devueltas,
       SUM(CASE WHEN estado = 'CANCELADA' THEN 1 ELSE 0 END) AS transacciones_canceladas,
       ROUND(100 * SUM(CASE WHEN estado = 'DEVUELTA'  THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                         AS tasa_devolucion_pct,
       ROUND(100 * SUM(CASE WHEN estado = 'CANCELADA' THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                         AS tasa_cancelacion_pct
FROM   ventas
GROUP  BY canal, categoria
ORDER  BY canal, categoria
"""


def consulta_2_tasas_devolucion_cancelacion(base: DataFrame, spark: SparkSession) -> DataFrame:
    """Tasa de devoluciones y cancelaciones por canal y categoría.

    pandas necesita tres groupBy y dos merge; en SQL basta una pasada con CASE WHEN.
    """
    base.createOrReplaceTempView("ventas")
    return spark.sql(SQL_CONSULTA_2)


# ---------------------------------------------------------------------------
# Consulta 3 - Función de ventana
# ---------------------------------------------------------------------------
def consulta_3_top10_productos(base: DataFrame, n: int = 10) -> DataFrame:
    """Top 10 productos por unidades vendidas (completadas) en cada departamento de la tienda.

    `rank()` equivale a `rank(method="min")` de pandas: empates comparten puesto.
    """
    por_producto = (
        base.where(F.col("estado") == "COMPLETADA")
        .groupBy("departamento", "producto_id")
        .agg(F.sum("cantidad").cast("long").alias("unidades_vendidas"))
    )
    ventana = Window.partitionBy("departamento").orderBy(F.col("unidades_vendidas").desc())
    return (
        por_producto.withColumn("ranking", F.rank().over(ventana))
        .where(F.col("ranking") <= n)
        .orderBy("departamento", "ranking", "producto_id")
    )


# ---------------------------------------------------------------------------
# Consulta 4 - Join con clientes + conteo distinto
# ---------------------------------------------------------------------------
def consulta_4_ticket_clientes(base: DataFrame, clientes: DataFrame) -> DataFrame:
    """Ticket promedio y clientes únicos por segmento de fidelización y mes (completadas).

    El LEFT JOIN con clientes (5 M filas) se hace aquí y solo con 3 columnas.
    Las compras sin cliente quedan con segmento 'Sin fidelización' y 0 clientes únicos.
    """
    compras = base.where(F.col("estado") == "COMPLETADA").select("cliente_id", "anio_mes", "venta_neta")
    con_segmento = compras.join(clientes.select("cliente_id", "segmento"), on="cliente_id", how="left")
    return (
        con_segmento.withColumn("segmento", F.coalesce(F.col("segmento"), F.lit(SIN_FIDELIZACION)))
        .groupBy("segmento", "anio_mes")
        .agg(
            F.round(F.avg("venta_neta"), 2).alias("ticket_promedio"),
            F.countDistinct("cliente_id").alias("clientes_unicos"),
            F.count(F.lit(1)).alias("total_transacciones"),
            F.sum("venta_neta").alias("venta_total"),
        )
        .orderBy("segmento", "anio_mes")
    )


def ejecutar_todas_las_consultas(
    base: DataFrame, clientes: DataFrame, spark: SparkSession
) -> dict[str, DataFrame]:
    """Construye los 4 planes (todavía sin ejecutar). Mismos nombres que en pandas."""
    return {
        "pregunta_1_ventas_margen": consulta_1_ventas_margen(base),
        "pregunta_2_tasas_devolucion_cancelacion": consulta_2_tasas_devolucion_cancelacion(base, spark),
        "pregunta_3_top10_productos": consulta_3_top10_productos(base),
        "pregunta_4_ticket_clientes": consulta_4_ticket_clientes(base, clientes),
    }
