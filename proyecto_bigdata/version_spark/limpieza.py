"""
version_spark/limpieza.py
=========================
Limpieza de la tabla de hechos con funciones nativas de Spark SQL.

Resuelve los mismos problemas de calidad que `version_pandas/limpieza.py`, en el mismo
orden, para que los resultados sean comparables:

  1. Duplicados (0,3 %): `dropDuplicates` es GLOBAL. Spark redistribuye las filas por
     hash de `transaccion_id` (shuffle), de modo que todas las copias de un id caen en
     la misma partición sin importar en qué archivo estaban. Es justo lo que la versión
     pandas por chunks no puede hacer sin guardar todos los ids en memoria.
  2. Cantidad <= 0 (0,3 %): se descarta.
  3. Precio nulo o <= 0 (0,2 %): se descarta.
  4. Método de pago con variantes (3 %): minúsculas, sin espacios extra y sin tildes,
     y luego un diccionario al valor canónico.
  5. Fechas en dos formatos (0,5 % dd/mm/aaaa HH:MM): se elige el patrón según la forma
     del texto, sin adivinar. (pandas con `format="mixed"` lee 07/09/2023 como 9 de julio;
     aquí se lee como 7 de septiembre.)
  6. Cliente nulo (8 %): se conserva; las compras anónimas cuentan en ventas.

Todo se expresa con columnas (`pyspark.sql.functions`), no con UDF de Python: así el
trabajo se queda dentro de la JVM, Catalyst lo optimiza y no hay que serializar cada
fila hacia un proceso Python.
"""

from __future__ import annotations

from itertools import chain

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Clave normalizada (minúsculas, sin tildes, espacios simples) -> valor canónico
MAPA_METODOS_PAGO = {
    "efectivo": "Efectivo",
    "tarjeta debito": "Tarjeta débito",
    "tarjeta credito": "Tarjeta crédito",
    "nequi": "Nequi",
    "daviplata": "Daviplata",
    "pse": "PSE",
}

FORMATO_FECHA_ISO = "yyyy-MM-dd HH:mm:ss"
FORMATO_FECHA_ALT = "dd/MM/yyyy HH:mm"


def normalizar_metodo_pago(col: Column) -> Column:
    """'  TARJETA Credito ' -> 'Tarjeta crédito'. Valores desconocidos -> Título."""
    clave = F.translate(F.lower(F.trim(col)), "áéíóú", "aeiou")
    clave = F.regexp_replace(clave, r"\s+", " ")
    mapa = F.create_map(*[F.lit(x) for x in chain.from_iterable(MAPA_METODOS_PAGO.items())])
    return F.coalesce(mapa[clave], F.initcap(clave))


def parsear_fecha(col: Column) -> Column:
    """Texto con dos formatos posibles -> timestamp. Texto nulo o inválido -> nulo."""
    return (
        F.when(col.contains("/"), F.to_timestamp(col, FORMATO_FECHA_ALT))
        .otherwise(F.to_timestamp(col, FORMATO_FECHA_ISO))
    )


def limpiar_transacciones(df: DataFrame, deduplicar: bool = True) -> DataFrame:
    """Aplica la limpieza completa. Devuelve un DataFrame (plan), sin ejecutar nada aún.

    Columnas añadidas: `anio_mes` ('YYYY-MM', clave de las consultas) y `anio`, `mes`
    (enteros, usados para particionar el Parquet de salida).
    """
    if deduplicar:
        df = df.dropDuplicates(["transaccion_id"])

    fecha = parsear_fecha(F.col("fecha_hora"))
    return (
        df.where((F.col("cantidad") > 0)
                 & F.col("precio_unitario").isNotNull()
                 & (F.col("precio_unitario") > 0))
        .withColumn("metodo_pago", normalizar_metodo_pago(F.col("metodo_pago")))
        .withColumn("fecha_hora", fecha)
        .where(F.col("fecha_hora").isNotNull())
        .withColumn("anio_mes", F.date_format("fecha_hora", "yyyy-MM"))
        .withColumn("anio", F.year("fecha_hora").cast("short"))
        .withColumn("mes", F.month("fecha_hora").cast("byte"))
    )


def perfil_calidad(df_crudo: DataFrame) -> dict[str, int]:
    """Cuenta los problemas de calidad del CSV crudo en UNA sola pasada (una agregación).

    Sirve como evidencia para el informe: muestra cuánto elimina cada regla de limpieza.
    Es una acción (lee los datos), así que solo se ejecuta si se pide (`--perfil`).
    """
    fila = df_crudo.agg(
        F.count(F.lit(1)).alias("filas"),
        F.countDistinct("transaccion_id").alias("ids_distintos"),
        F.sum(F.when(F.col("cantidad") <= 0, 1).otherwise(0)).alias("cantidad_no_positiva"),
        F.sum(F.when(F.col("precio_unitario").isNull(), 1).otherwise(0)).alias("precio_nulo"),
        F.sum(F.when(F.col("fecha_hora").isNull(), 1).otherwise(0)).alias("fecha_nula"),
        F.sum(F.when(F.col("fecha_hora").contains("/"), 1).otherwise(0)).alias("fecha_formato_alt"),
        F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("cliente_nulo"),
        F.sum(F.when(~F.col("metodo_pago").isin(list(MAPA_METODOS_PAGO.values())), 1)
              .otherwise(0)).alias("metodo_pago_sucio"),
    ).first().asDict()
    fila["duplicados"] = fila["filas"] - fila["ids_distintos"]
    return {k: int(v or 0) for k, v in fila.items()}
