"""
comun/esquemas.py
=================
Esquemas de los datos, compartidos por la versión pandas y la versión Spark.

- En Spark se pasa el esquema explícito: evita `inferSchema=True`, que obliga a
  leer TODO el CSV una vez más solo para adivinar los tipos (en 20+ GB son minutos).
- En pandas se pasan `dtype` y `usecols` optimizados (mitigación exigida en §4 del
  enunciado: "tipos de datos optimizados, selección de columnas").

`fecha_hora` se lee como texto en ambos casos porque viene con dos formatos
(aaaa-mm-dd HH:MM:SS y dd/mm/aaaa HH:MM) y se normaliza en la etapa de limpieza.
"""

from __future__ import annotations

# ----------------------------------------------------------------------------
# pandas
# ----------------------------------------------------------------------------
# cliente_id y precio_unitario tienen nulos -> tipos "nullable" (Int32 / float32)
DTYPES_PANDAS_TRANSACCIONES = {
    "transaccion_id": "int64",
    "fecha_hora": "string",
    "cliente_id": "Int32",
    "producto_id": "int32",
    "tienda_id": "int16",
    "canal": "category",
    "cantidad": "int16",
    "precio_unitario": "float32",
    "descuento_pct": "int8",
    "metodo_pago": "string",   # no 'category' aún: tiene variantes sucias
    "estado": "category",
}
DTYPES_PANDAS_CLIENTES = {
    "cliente_id": "int32", "ciudad": "category", "departamento": "category",
    "segmento": "category", "genero": "category", "edad": "Int16",
    "fecha_registro": "string", "acepta_marketing": "bool",
}
DTYPES_PANDAS_PRODUCTOS = {
    "producto_id": "int32", "nombre_producto": "string", "categoria": "category",
    "subcategoria": "category", "marca": "category",
    "precio_lista": "float32", "costo_unitario": "float32",
}
DTYPES_PANDAS_TIENDAS = {
    "tienda_id": "int16", "nombre_tienda": "string", "tipo_tienda": "category",
    "ciudad": "category", "departamento": "category", "region": "category",
    "fecha_apertura": "string", "area_m2": "int32",
}


# ----------------------------------------------------------------------------
# PySpark (import perezoso para que pandas no dependa de pyspark)
# ----------------------------------------------------------------------------
def esquema_spark_transacciones():
    from pyspark.sql.types import (ByteType, DoubleType, IntegerType, LongType, ShortType,
                                   StringType, StructField, StructType)
    return StructType([
        StructField("transaccion_id", LongType(), False),
        StructField("fecha_hora", StringType(), True),
        StructField("cliente_id", IntegerType(), True),
        StructField("producto_id", IntegerType(), False),
        StructField("tienda_id", ShortType(), False),
        StructField("canal", StringType(), False),
        StructField("cantidad", ShortType(), False),
        StructField("precio_unitario", DoubleType(), True),
        StructField("descuento_pct", ByteType(), False),
        StructField("metodo_pago", StringType(), False),
        StructField("estado", StringType(), False),
    ])


def esquema_spark_clientes():
    from pyspark.sql.types import (BooleanType, DateType, IntegerType, ShortType, StringType,
                                   StructField, StructType)
    return StructType([
        StructField("cliente_id", IntegerType(), False),
        StructField("ciudad", StringType()),
        StructField("departamento", StringType()),
        StructField("segmento", StringType()),
        StructField("genero", StringType()),
        StructField("edad", ShortType()),
        StructField("fecha_registro", DateType()),
        StructField("acepta_marketing", BooleanType()),
    ])


def esquema_spark_productos():
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
    return StructType([
        StructField("producto_id", IntegerType(), False),
        StructField("nombre_producto", StringType()),
        StructField("categoria", StringType()),
        StructField("subcategoria", StringType()),
        StructField("marca", StringType()),
        StructField("precio_lista", DoubleType()),
        StructField("costo_unitario", DoubleType()),
    ])


def esquema_spark_tiendas():
    from pyspark.sql.types import (DateType, IntegerType, ShortType, StringType, StructField,
                                   StructType)
    return StructType([
        StructField("tienda_id", ShortType(), False),
        StructField("nombre_tienda", StringType()),
        StructField("tipo_tienda", StringType()),
        StructField("ciudad", StringType()),
        StructField("departamento", StringType()),
        StructField("region", StringType()),
        StructField("fecha_apertura", DateType()),
        StructField("area_m2", IntegerType()),
    ])
