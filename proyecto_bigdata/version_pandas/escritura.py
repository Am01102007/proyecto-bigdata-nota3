"""
version_pandas/escritura.py
===========================
Persistencia de los resultados analíticos en formato columnar Parquet.
Cumple con el requisito de escritura eficiente y almacenamiento analítico.
"""

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
DIR_RESULTADOS_PANDAS = RAIZ / "resultados" / "pandas"


def guardar_resultados_parquet(
    resultados: dict[str, pd.DataFrame],
    directorio_salida: Path = DIR_RESULTADOS_PANDAS,
    compression: str = "snappy",
) -> dict[str, Path]:
    """Guarda cada consulta analítica en formato Parquet.

    Args:
        resultados: Diccionario {nombre_consulta: DataFrame}.
        directorio_salida: Directorio destino (por defecto resultados/pandas/).
        compression: Algoritmo de compresión ('snappy' o 'gzip').

    Returns:
        Diccionario {nombre_consulta: ruta_archivo_guardado}.
    """
    directorio_salida.mkdir(parents=True, exist_ok=True)
    rutas_generadas: dict[str, Path] = {}

    for nombre, df in resultados.items():
        ruta_archivo = directorio_salida / f"{nombre}.parquet"
        df.to_parquet(
            ruta_archivo,
            engine="pyarrow",
            compression=compression,
            index=False,
        )
        rutas_generadas[nombre] = ruta_archivo

    return rutas_generadas
