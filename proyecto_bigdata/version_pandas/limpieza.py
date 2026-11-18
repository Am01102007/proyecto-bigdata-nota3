"""
version_pandas/limpieza.py
==========================
Rutinas de depuración y saneamiento de datos para la versión pandas.
Resuelve los 6 problemas de calidad simulados en el conjunto sintético:
  1. Deduplicación de transacciones (0.3 % de reenvíos POS).
  2. Parseo y normalización de fechas mixtas (aaaa-mm-dd HH:MM:SS vs. dd/mm/aaaa HH:MM).
  3. Filtrado de cantidades erróneas (<= 0).
  4. Filtrado o descarte de precios unitarios nulos / no válidos.
  5. Estandarización de cadenas del método de pago (variantes con mayúsculas/espacios/sin tildes).
  6. Preservación controlada de compras anónimas (cliente_id nulo para LEFT JOIN).
"""

from __future__ import annotations

import re
import pandas as pd

# Mapeo canónico de métodos de pago sucios a valores oficiales
MAPA_METODOS_PAGO = {
    "efectivo": "Efectivo",
    "tarjeta debito": "Tarjeta débito",
    "tarjeta débito": "Tarjeta débito",
    "tarjeta credito": "Tarjeta crédito",
    "tarjeta crédito": "Tarjeta crédito",
    "nequi": "Nequi",
    "daviplata": "Daviplata",
    "pse": "PSE",
}


def normalizar_metodo_pago(serie: pd.Series) -> pd.Series:
    """Estandariza los nombres de métodos de pago eliminando variaciones de digitación."""
    s = serie.astype(str).str.strip().str.lower()
    # Limpieza de dobles espacios
    s = s.str.replace(r"\s+", " ", regex=True)
    # Reemplazo por valor canónico
    return s.map(lambda x: MAPA_METODOS_PAGO.get(x, x.title()))


def normalizar_fechas(serie: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Parsea fechas con formatos mixtos (ISO y formato colombiano día/mes/año).

    Returns:
        Tupla (serie_datetime, serie_año_mes):
        - serie_datetime: Timestamp normalizado
        - serie_año_mes: String 'YYYY-MM' para agrupaciones mensuales de negocio
    """
    # Cada formato se lee con su patrón explícito. format="mixed" interpretaba
    # 07/09/2023 como 9 de julio (mes/día) en lugar de 7 de septiembre.
    s = serie.astype("string")
    alt = s.str.contains("/", na=False)          # formato dd/mm/aaaa HH:MM
    dt = pd.to_datetime(s.where(~alt), format="%Y-%m-%d %H:%M:%S", errors="coerce")
    dt = dt.fillna(pd.to_datetime(s.where(alt), format="%d/%m/%Y %H:%M", errors="coerce"))
    año_mes = dt.dt.to_period("M").astype(str)
    return dt, año_mes


def limpiar_transacciones(
    df: pd.DataFrame,
    deduplicar: bool = True,
) -> pd.DataFrame:
    """Aplica el pipeline integral de limpieza sobre un DataFrame de transacciones.

    Args:
        df: DataFrame de transacciones con columnas crudas.
        deduplicar: Si True, elimina duplicados en transaccion_id.
                    (Nota: dentro de un chunk solo deduplica localmente).

    Returns:
        DataFrame limpio, con fecha_hora normalizada y columna adicional 'anio_mes'.
    """
    if df.empty:
        return df

    df_limpio = df.copy()

    # 1. Deduplicación
    if deduplicar and "transaccion_id" in df_limpio.columns:
        df_limpio = df_limpio.drop_duplicates(subset=["transaccion_id"], keep="first")

    # 2. Filtrado de cantidades positivas válidas
    if "cantidad" in df_limpio.columns:
        df_limpio = df_limpio[df_limpio["cantidad"] > 0]

    # 3. Filtrado de precios unitarios válidos (no nulos y > 0)
    if "precio_unitario" in df_limpio.columns:
        df_limpio = df_limpio[df_limpio["precio_unitario"].notna() & (df_limpio["precio_unitario"] > 0)]

    # 4. Normalización de método de pago
    if "metodo_pago" in df_limpio.columns:
        df_limpio["metodo_pago"] = normalizar_metodo_pago(df_limpio["metodo_pago"])

    # 5. Normalización de fechas y derivación de periodo mensual
    if "fecha_hora" in df_limpio.columns:
        dt, anio_mes = normalizar_fechas(df_limpio["fecha_hora"])
        df_limpio["fecha_hora"] = dt
        df_limpio["anio_mes"] = anio_mes
        # Descartar filas con fecha completamente corrupta
        df_limpio = df_limpio[df_limpio["fecha_hora"].notna()]

    return df_limpio
