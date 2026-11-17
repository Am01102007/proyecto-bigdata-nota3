"""
version_pandas
==============
Implementación completa del pipeline de procesamiento de ventas retail omnicanal
utilizando pandas (versión monocore / tradicional para comparación contra Apache Spark).

Módulos:
    - cargador: lectura de dimensiones y transacciones (estándar y por chunks).
    - limpieza: deduplicación, parseo de fechas mixtas, filtrado de cantidades y normalización de textos.
    - joins: cruce con dimensiones productos, tiendas y clientes (left join).
    - agregaciones: cálculo de las 4 consultas de analítica de negocio.
    - escritura: guardado en formato columnar Parquet particionado/consolidado.
    - mitigaciones: aplicación y análisis de técnicas de contención (chunks, dtypes, usecols).
    - main: punto de entrada CLI para ejecución por muestras y benchmarking.
"""

__author__ = "Samuel Zuluaga"
__version__ = "1.0.0"
