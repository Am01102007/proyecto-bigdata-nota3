"""
version_spark
=============
Implementación del pipeline de ventas retail omnicanal con Apache Spark (PySpark 3.5).

Procesa los mismos datos y responde las mismas 4 preguntas de negocio que
`version_pandas`, pero con el modelo de ejecución de Spark: particiones procesadas
en paralelo, evaluación perezosa (plan optimizado por Catalyst antes de ejecutar)
y spill a disco cuando la memoria no alcanza.

Módulos:
    - sesion: creación de la SparkSession con la configuración para modo local.
    - cargador: lectura de CSV con esquema explícito (sin inferSchema).
    - limpieza: deduplicación global, fechas en dos formatos, filtros y normalización.
    - joins: broadcast join con productos y tiendas; join con redistribución con clientes.
    - agregaciones: las 4 consultas (API de DataFrames, Spark SQL y funciones de ventana).
    - escritura: resultados en Parquet y datos limpios en Parquet particionado por año/mes.
    - main: punto de entrada CLI y función `ejecutar_pipeline` reutilizable por el benchmark.
    - validar_equivalencia: compara los resultados de Spark con los de pandas.
"""

__author__ = "Juan Pablo Rodriguez Gamba"
__version__ = "1.0.0"
