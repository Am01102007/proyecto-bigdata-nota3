"""
version_pandas/cargador.py
==========================
Carga de datos desde archivos CSV hacia DataFrames de pandas.
Soporta dos modalidades de ejecución:
  1. Carga ingenua / directa (sin mitigaciones): inferencia automática de dtypes,
     lectura de todas las columnas y concatenación directa en memoria.
  2. Carga mitigada: especificación estricta de dtypes optimizados (category, int8,
     int16, float32, Int32 nullable), selección selectiva de columnas (usecols)
     y generador por fragmentos (chunksize).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Generator, Sequence

import pandas as pd

# Asegurar importación de módulos comunes
RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from comun.esquemas import (
    DTYPES_PANDAS_CLIENTES,
    DTYPES_PANDAS_PRODUCTOS,
    DTYPES_PANDAS_TIENDAS,
    DTYPES_PANDAS_TRANSACCIONES,
)
from comun.muestras import DIR_DIMENSIONES, rutas_muestra


def cargar_dimensiones(
    mitigado: bool = True,
    dir_dimensiones: Path = DIR_DIMENSIONES,
) -> dict[str, pd.DataFrame]:
    """Carga los tres maestros dimensionales: productos, tiendas y clientes.

    Args:
        mitigado: Si True, usa dtypes optimizados de esquemas.py.
                  Si False, permite que pandas infiera los tipos (mayor consumo de RAM).
        dir_dimensiones: Carpeta que contiene tiendas.csv, productos.csv y clientes.csv.

    Returns:
        Diccionario con las claves 'tiendas', 'productos', 'clientes'.
    """
    ruta_tiendas = dir_dimensiones / "tiendas.csv"
    ruta_productos = dir_dimensiones / "productos.csv"
    ruta_clientes = dir_dimensiones / "clientes.csv"

    if not (ruta_tiendas.exists() and ruta_productos.exists() and ruta_clientes.exists()):
        raise FileNotFoundError(
            f"Faltan archivos dimensionales en {dir_dimensiones}. Ejecute primero scripts/generar_datos.py"
        )

    if mitigado:
        tiendas = pd.read_csv(ruta_tiendas, dtype=DTYPES_PANDAS_TIENDAS)
        productos = pd.read_csv(ruta_productos, dtype=DTYPES_PANDAS_PRODUCTOS)
        clientes = pd.read_csv(ruta_clientes, dtype=DTYPES_PANDAS_CLIENTES)
    else:
        # Carga sin tipos predefinidos (incurre en overhead de tipos int64/float64/object)
        tiendas = pd.read_csv(ruta_tiendas)
        productos = pd.read_csv(ruta_productos)
        clientes = pd.read_csv(ruta_clientes)

    return {
        "tiendas": tiendas,
        "productos": productos,
        "clientes": clientes,
    }


def cargar_transacciones_completas(
    rutas: Sequence[Path],
    mitigado: bool = False,
    usecols: list[str] | None = None,
) -> pd.DataFrame:
    """Carga y concatena en memoria todos los archivos de transacciones especificados.

    Esta función representa el enfoque tradicional de pandas. Con muestras grandes
    (> 10 % o 50 %), esta llamada puede agotar la memoria y lanzar un MemoryError.

    Args:
        rutas: Lista de rutas a los archivos part-*.csv.
        mitigado: Si True, aplica dtypes optimizados y low_memory=False.
        usecols: Lista opcional de columnas a leer (proyección en lectura).
    """
    dataframes: list[pd.DataFrame] = []
    dtypes = DTYPES_PANDAS_TRANSACCIONES if mitigado else None

    # Si hay usecols y mitigado, filtrar los dtypes solo a las columnas solicitadas
    if mitigado and dtypes and usecols:
        dtypes = {col: dtypes[col] for col in usecols if col in dtypes}

    for ruta in rutas:
        df = pd.read_csv(
            ruta,
            dtype=dtypes,
            usecols=usecols,
            engine="c",
        )
        dataframes.append(df)

    if not dataframes:
        return pd.DataFrame()

    return pd.concat(dataframes, ignore_index=True)


def iterar_transacciones_chunks(
    rutas: Sequence[Path],
    chunksize: int = 250_000,
    mitigado: bool = True,
    usecols: list[str] | None = None,
) -> Generator[pd.DataFrame, None, None]:
    """Generador que produce fragmentos (chunks) de DataFrames a partir de los CSVs.

    Técnica de mitigación en pandas para evitar cargar todo el dataset a la vez.

    Args:
        rutas: Lista de rutas a los archivos part-*.csv.
        chunksize: Número de filas por fragmento.
        mitigado: Aplica tipos optimizados de comun.esquemas si True.
        usecols: Subconjunto de columnas a cargar.
    """
    dtypes = DTYPES_PANDAS_TRANSACCIONES if mitigado else None
    if mitigado and dtypes and usecols:
        dtypes = {col: dtypes[col] for col in usecols if col in dtypes}

    for ruta in rutas:
        lector = pd.read_csv(
            ruta,
            dtype=dtypes,
            usecols=usecols,
            chunksize=chunksize,
            engine="c",
        )
        for chunk in lector:
            yield chunk
