"""
version_pandas/joins.py
=======================
Operaciones de cruce relacional (JOINs) entre la tabla de hechos (transacciones)
y los maestros dimensionales según el esquema en estrella.

Diseño de joins:
  - productos: INNER JOIN (60.000 filas). Enriquecimiento con categoría, costo y subcategoría.
  - tiendas:   INNER JOIN (600 filas). Enriquecimiento con región, departamento y formato.
  - clientes:  LEFT JOIN (5.000.000 filas). 8 % de transacciones son anónimas (cliente_id nulo);
               el LEFT JOIN es mandatorio para no perder volumen transaccional de ventas.
"""

from __future__ import annotations

import pandas as pd


def unir_productos(
    df_transacciones: pd.DataFrame,
    df_productos: pd.DataFrame,
    columnas_producto: list[str] | None = None,
) -> pd.DataFrame:
    """Realiza INNER JOIN entre transacciones y el maestro de productos.

    Args:
        df_transacciones: DataFrame de ventas limpias.
        df_productos: Maestro de productos.
        columnas_producto: Lista de atributos a incorporar (por defecto categoría, costo y subcategoría).
    """
    if columnas_producto is None:
        columnas_producto = ["producto_id", "categoria", "subcategoria", "costo_unitario"]

    cols_disponibles = [c for c in columnas_producto if c in df_productos.columns]
    if "producto_id" not in cols_disponibles:
        cols_disponibles.insert(0, "producto_id")

    return df_transacciones.merge(
        df_productos[cols_disponibles],
        on="producto_id",
        how="inner",
    )


def unir_tiendas(
    df_transacciones: pd.DataFrame,
    df_tiendas: pd.DataFrame,
    columnas_tienda: list[str] | None = None,
) -> pd.DataFrame:
    """Realiza INNER JOIN entre transacciones y el maestro de tiendas.

    Args:
        df_transacciones: DataFrame enriquecido de transacciones.
        df_tiendas: Maestro de tiendas físicas y centros de distribución.
        columnas_tienda: Atributos a incorporar (por defecto región, departamento y ciudad).
    """
    if columnas_tienda is None:
        columnas_tienda = ["tienda_id", "region", "departamento", "ciudad", "tipo_tienda"]

    cols_disponibles = [c for c in columnas_tienda if c in df_tiendas.columns]
    if "tienda_id" not in cols_disponibles:
        cols_disponibles.insert(0, "tienda_id")

    return df_transacciones.merge(
        df_tiendas[cols_disponibles],
        on="tienda_id",
        how="inner",
        suffixes=("", "_tienda"),
    )


def unir_clientes(
    df_transacciones: pd.DataFrame,
    df_clientes: pd.DataFrame,
    columnas_cliente: list[str] | None = None,
) -> pd.DataFrame:
    """Realiza LEFT JOIN entre transacciones y clientes.

    El 8 % de transacciones tiene cliente_id nulo (compras sin tarjeta de fidelización).
    El LEFT JOIN garantiza que estas compras no se descarten del análisis contable.
    """
    if columnas_cliente is None:
        columnas_cliente = ["cliente_id", "segmento", "departamento", "genero"]

    cols_disponibles = [c for c in columnas_cliente if c in df_clientes.columns]
    if "cliente_id" not in cols_disponibles:
        cols_disponibles.insert(0, "cliente_id")

    return df_transacciones.merge(
        df_clientes[cols_disponibles],
        on="cliente_id",
        how="left",
        suffixes=("", "_cliente"),
    )


def unir_modelo_estrella(
    df_transacciones: pd.DataFrame,
    dimensiones: dict[str, pd.DataFrame],
    incluir_clientes: bool = True,
) -> pd.DataFrame:
    """Aplica todos los cruces del modelo en estrella sobre las transacciones."""
    df_enriquecido = unir_productos(df_transacciones, dimensiones["productos"])
    df_enriquecido = unir_tiendas(df_enriquecido, dimensiones["tiendas"])

    if incluir_clientes and "clientes" in dimensiones:
        df_enriquecido = unir_clientes(df_enriquecido, dimensiones["clientes"])

    return df_enriquecido
