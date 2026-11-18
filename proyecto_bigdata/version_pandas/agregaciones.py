"""
version_pandas/agregaciones.py
==============================
Implementación de las 4 consultas analíticas de negocio formuladas en el proyecto:

1. Ventas netas, unidades y margen bruto por mes, región y categoría.
2. Tasa de devoluciones y cancelaciones por canal y categoría.
3. Top 10 productos más vendidos en cada departamento (ranking intra-grupo).
4. Ticket promedio y clientes únicos por segmento de fidelización y mes.
"""

from __future__ import annotations

import pandas as pd


def calcular_metricas_financieras(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula columnas financieras derivadas: precio cobrado, venta neta y margen bruto."""
    df_calc = df.copy()

    # Descuento en escala [0, 1]
    pct_desc = df_calc["descuento_pct"].astype("float32") / 100.0
    precio_cobrado = df_calc["precio_unitario"].astype("float32") * (1.0 - pct_desc)
    df_calc["precio_cobrado"] = precio_cobrado

    # Venta neta = unidades * precio cobrado
    df_calc["venta_neta"] = df_calc["cantidad"].astype("float32") * precio_cobrado

    # Margen bruto = venta neta - (cantidad * costo unitario)
    if "costo_unitario" in df_calc.columns:
        costo_total = df_calc["cantidad"].astype("float32") * df_calc["costo_unitario"].astype("float32")
        df_calc["costo_total"] = costo_total
        df_calc["margen_bruto"] = df_calc["venta_neta"] - costo_total
    else:
        df_calc["margen_bruto"] = 0.0

    return df_calc


def consulta_1_ventas_margen(df_unido: pd.DataFrame) -> pd.DataFrame:
    """Pregunta 1: Ventas netas, unidades y margen bruto por mes, región y categoría.

    Solo considera transacciones válidas no canceladas.
    """
    df_valido = df_unido[df_unido["estado"] != "CANCELADA"]
    df_valido = calcular_metricas_financieras(df_valido)

    agrupado = (
        df_valido.groupby(["anio_mes", "region", "categoria"], observed=True)
        .agg(
            unidades_vendidas=("cantidad", "sum"),
            venta_neta_total=("venta_neta", "sum"),
            margen_bruto_total=("margen_bruto", "sum"),
        )
        .reset_index()
    )

    agrupado["margen_pct"] = (
        (agrupado["margen_bruto_total"] / agrupado["venta_neta_total"] * 100)
        .round(2)
        .fillna(0.0)
    )

    return agrupado.sort_values(["anio_mes", "region", "categoria"]).reset_index(drop=True)


def consulta_2_tasas_devolucion_cancelacion(df_unido: pd.DataFrame) -> pd.DataFrame:
    """Pregunta 2: Tasa de devoluciones y cancelaciones por canal y categoría."""
    total_trans = (
        df_unido.groupby(["canal", "categoria"], observed=True)
        .size()
        .reset_index(name="total_transacciones")
    )

    devueltas = (
        df_unido[df_unido["estado"] == "DEVUELTA"]
        .groupby(["canal", "categoria"], observed=True)
        .size()
        .reset_index(name="transacciones_devueltas")
    )

    canceladas = (
        df_unido[df_unido["estado"] == "CANCELADA"]
        .groupby(["canal", "categoria"], observed=True)
        .size()
        .reset_index(name="transacciones_canceladas")
    )

    res = total_trans.merge(devueltas, on=["canal", "categoria"], how="left")
    res["transacciones_devueltas"] = res["transacciones_devueltas"].fillna(0)  # solo numéricas: canal/categoria pueden ser category
    res = res.merge(canceladas, on=["canal", "categoria"], how="left")
    res["transacciones_canceladas"] = res["transacciones_canceladas"].fillna(0)

    res["tasa_devolucion_pct"] = (
        (res["transacciones_devueltas"] / res["total_transacciones"] * 100).round(2)
    )
    res["tasa_cancelacion_pct"] = (
        (res["transacciones_canceladas"] / res["total_transacciones"] * 100).round(2)
    )

    return res.sort_values(["canal", "categoria"]).reset_index(drop=True)


def consulta_3_top10_productos(df_unido: pd.DataFrame) -> pd.DataFrame:
    """Pregunta 3: Los 10 productos más vendidos en cada departamento.

    Ranking relativo por cantidad vendida dentro de cada departamento geográfico.
    """
    df_completadas = df_unido[df_unido["estado"] == "COMPLETADA"]

    # Agrupar por departamento y producto
    col_depto = "departamento" if "departamento" in df_completadas.columns else "departamento_tienda"
    agrupado = (
        df_completadas.groupby([col_depto, "producto_id"], observed=True)
        .agg(unidades_vendidas=("cantidad", "sum"))
        .reset_index()
    )

    # Calcular ranking descendente dentro de cada departamento
    agrupado["ranking"] = (
        agrupado.groupby(col_depto, observed=True)["unidades_vendidas"]
        .rank(method="min", ascending=False)
        .astype(int)
    )

    # Filtrar solo el Top 10
    top10 = agrupado[agrupado["ranking"] <= 10].sort_values(
        [col_depto, "ranking"], ascending=[True, True]
    )

    return top10.reset_index(drop=True)


def consulta_4_ticket_clientes_fidelizacion(df_unido: pd.DataFrame) -> pd.DataFrame:
    """Pregunta 4: Ticket promedio y clientes únicos por segmento de fidelización y mes.

    Nota crítica de Big Data:
    `nunique` sobre cliente_id es una métrica no distributiva / no aditiva.
    """
    df_valido = df_unido[df_unido["estado"] == "COMPLETADA"].copy()
    df_valido = calcular_metricas_financieras(df_valido)

    # Excluir compras anónimas o catalogarlas como 'Sin fidelización'
    if "segmento" in df_valido.columns:
        if isinstance(df_valido["segmento"].dtype, pd.CategoricalDtype):
            if "Sin fidelización" not in df_valido["segmento"].cat.categories:
                df_valido["segmento"] = df_valido["segmento"].cat.add_categories(["Sin fidelización"])
        df_valido["segmento"] = df_valido["segmento"].fillna("Sin fidelización")
    else:
        df_valido["segmento"] = "Desconocido"

    agrupado = (
        df_valido.groupby(["segmento", "anio_mes"], observed=True)
        .agg(
            ticket_promedio=("venta_neta", "mean"),
            clientes_unicos=("cliente_id", "nunique"),
            total_transacciones=("transaccion_id", "count"),
            venta_total=("venta_neta", "sum"),
        )
        .reset_index()
    )

    agrupado["ticket_promedio"] = agrupado["ticket_promedio"].round(2)
    return agrupado.sort_values(["segmento", "anio_mes"]).reset_index(drop=True)


def ejecutar_todas_las_consultas(df_enriquecido: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Ejecuta en lote las 4 consultas analíticas sobre el DataFrame enriquecido."""
    return {
        "pregunta_1_ventas_margen": consulta_1_ventas_margen(df_enriquecido),
        "pregunta_2_tasas_devolucion_cancelacion": consulta_2_tasas_devolucion_cancelacion(df_enriquecido),
        "pregunta_3_top10_productos": consulta_3_top10_productos(df_enriquecido),
        "pregunta_4_ticket_clientes": consulta_4_ticket_clientes_fidelizacion(df_enriquecido),
    }
