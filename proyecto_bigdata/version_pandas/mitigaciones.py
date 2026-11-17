"""
version_pandas/mitigaciones.py
==============================
Implementación y análisis técnico de las técnicas de mitigación de pandas:
  1. Selección selectiva de columnas (usecols / proyección temprana).
  2. Tipos de datos optimizados (dtypes compactos: category, int8, int16, float32, Int32).
  3. Procesamiento por fragmentos (chunksize / streaming batch).

Este módulo implementa el pipeline con mitigaciones y expone las limitaciones
insalvables de pandas frente al volumen masivo de datos.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from comun.esquemas import DTYPES_PANDAS_TRANSACCIONES
from version_pandas.cargador import cargar_dimensiones, iterar_transacciones_chunks
from version_pandas.limpieza import limpiar_transacciones
from version_pandas.joins import unir_productos, unir_tiendas, unir_clientes
from version_pandas.agregaciones import (
    calcular_metricas_financieras,
    consulta_1_ventas_margen,
    consulta_2_tasas_devolucion_cancelacion,
    consulta_3_top10_productos,
    consulta_4_ticket_clientes_fidelizacion,
)


def columnas_optimizadas_por_consulta(consulta_id: int) -> list[str]:
    """Retorna la lista mínima de columnas necesarias para cada consulta analítica (usecols)."""
    base = ["transaccion_id", "fecha_hora", "estado", "producto_id", "tienda_id"]
    if consulta_id == 1:
        # Ventas, unidades y margen
        return base + ["cantidad", "precio_unitario", "descuento_pct"]
    elif consulta_id == 2:
        # Tasas de devoluciones y cancelaciones
        return ["transaccion_id", "canal", "producto_id", "estado"]
    elif consulta_id == 3:
        # Top 10 productos por departamento
        return ["transaccion_id", "producto_id", "tienda_id", "cantidad", "estado"]
    elif consulta_id == 4:
        # Ticket promedio y clientes únicos
        return ["transaccion_id", "fecha_hora", "cliente_id", "cantidad", "precio_unitario", "descuento_pct", "estado"]
    return [
        "transaccion_id", "fecha_hora", "cliente_id", "producto_id", "tienda_id",
        "canal", "cantidad", "precio_unitario", "descuento_pct", "metodo_pago", "estado",
    ]


def ejecutar_pipeline_mitigado_por_chunks(
    rutas: Sequence[Path],
    dimensiones: dict[str, pd.DataFrame],
    chunksize: int = 250_000,
) -> dict[str, pd.DataFrame]:
    """Ejecuta el procesamiento de las 4 consultas acumulando resultados por chunks.

    Muestra cómo pandas puede mitigar la memoria para operaciones aditivas simples,
    pero experimenta severas limitaciones en operaciones globales.
    """
    acum_q1: list[pd.DataFrame] = []
    acum_q2_total: list[pd.DataFrame] = []
    acum_q2_dev: list[pd.DataFrame] = []
    acum_q2_canc: list[pd.DataFrame] = []
    acum_q3: list[pd.DataFrame] = []

    # Para Q4 (clientes únicos):
    # Demuestra la inviabilidad de chunks: se deben acumular los pares (segmento, anio_mes, cliente_id)
    # o sets en memoria, pues nunique NO es una operación distributiva/aditiva.
    sets_clientes_unicos: dict[tuple[str, str], set[int]] = {}
    acum_q4_ventas: list[pd.DataFrame] = []

    # Iterador con dtypes y chunks
    iterador = iterar_transacciones_chunks(
        rutas=rutas,
        chunksize=chunksize,
        mitigado=True,
    )

    for chunk in iterador:
        # Limpieza local del chunk (nota: drop_duplicates aquí solo atrapa duplicados intra-chunk)
        chunk_limpio = limpiar_transacciones(chunk, deduplicar=True)
        if chunk_limpio.empty:
            continue

        # Cruces dimensionales
        enriquecido = unir_productos(chunk_limpio, dimensiones["productos"])
        enriquecido = unir_tiendas(enriquecido, dimensiones["tiendas"])
        enriquecido_clientes = unir_clientes(enriquecido, dimensiones["clientes"])

        # --- Acumulador Consulta 1 (Aditiva) ---
        q1_parcial = consulta_1_ventas_margen(enriquecido)
        acum_q1.append(q1_parcial)

        # --- Acumulador Consulta 2 (Conteos aditivos) ---
        q2_tot = enriquecido.groupby(["canal", "categoria"], observed=True).size().reset_index(name="total_transacciones")
        q2_d = enriquecido[enriquecido["estado"] == "DEVUELTA"].groupby(["canal", "categoria"], observed=True).size().reset_index(name="transacciones_devueltas")
        q2_c = enriquecido[enriquecido["estado"] == "CANCELADA"].groupby(["canal", "categoria"], observed=True).size().reset_index(name="transacciones_canceladas")
        acum_q2_total.append(q2_tot)
        acum_q2_dev.append(q2_d)
        acum_q2_canc.append(q2_c)

        # --- Acumulador Consulta 3 (Suma parcial por departamento y producto) ---
        df_comp = enriquecido[enriquecido["estado"] == "COMPLETADA"]
        col_dep = "departamento" if "departamento" in df_comp.columns else "departamento_tienda"
        q3_parcial = df_comp.groupby([col_dep, "producto_id"], observed=True)["cantidad"].sum().reset_index()
        acum_q3.append(q3_parcial)

        # --- Acumulador Consulta 4 (Métricas financieras + sets de clientes únicos) ---
        df_q4 = enriquecido_clientes[enriquecido_clientes["estado"] == "COMPLETADA"].copy()
        df_q4 = calcular_metricas_financieras(df_q4)
        if "segmento" in df_q4.columns:
            if isinstance(df_q4["segmento"].dtype, pd.CategoricalDtype):
                if "Sin fidelización" not in df_q4["segmento"].cat.categories:
                    df_q4["segmento"] = df_q4["segmento"].cat.add_categories(["Sin fidelización"])
            df_q4["segmento"] = df_q4["segmento"].fillna("Sin fidelización")
        else:
            df_q4["segmento"] = "Desconocido"

        # Ventas y transacciones se acumulan sumando
        q4_v = df_q4.groupby(["segmento", "anio_mes"], observed=True).agg(
            venta_total=("venta_neta", "sum"),
            total_transacciones=("transaccion_id", "count"),
        ).reset_index()
        acum_q4_ventas.append(q4_v)

        # Inviabilidad técnica de nunique:
        # Se deben almacenar los IDs de clientes en memoria principal
        df_validos = df_q4[df_q4["cliente_id"].notna()]
        if not df_validos.empty:
            for (seg, mes), grupo in df_validos.groupby(["segmento", "anio_mes"], observed=True)["cliente_id"]:
                clave = (str(seg), str(mes))
                if clave not in sets_clientes_unicos:
                    sets_clientes_unicos[clave] = set()
                sets_clientes_unicos[clave].update(grupo.values)

    # Consolidación final de la Consulta 1
    if acum_q1:
        c1 = pd.concat(acum_q1, ignore_index=True)
        res_q1 = c1.groupby(["anio_mes", "region", "categoria"], observed=True).agg(
            unidades_vendidas=("unidades_vendidas", "sum"),
            venta_neta_total=("venta_neta_total", "sum"),
            margen_bruto_total=("margen_bruto_total", "sum"),
        ).reset_index()
        res_q1["margen_pct"] = (res_q1["margen_bruto_total"] / res_q1["venta_neta_total"] * 100).round(2).fillna(0.0)
    else:
        res_q1 = pd.DataFrame()

    # Consolidación final de la Consulta 2
    if acum_q2_total:
        tot = pd.concat(acum_q2_total).groupby(["canal", "categoria"], observed=True)["total_transacciones"].sum().reset_index()
        dev = pd.concat(acum_q2_dev).groupby(["canal", "categoria"], observed=True)["transacciones_devueltas"].sum().reset_index() if acum_q2_dev else pd.DataFrame(columns=["canal", "categoria", "transacciones_devueltas"])
        canc = pd.concat(acum_q2_canc).groupby(["canal", "categoria"], observed=True)["transacciones_canceladas"].sum().reset_index() if acum_q2_canc else pd.DataFrame(columns=["canal", "categoria", "transacciones_canceladas"])

        res_q2 = tot.merge(dev, on=["canal", "categoria"], how="left").fillna(0)
        res_q2 = res_q2.merge(canc, on=["canal", "categoria"], how="left").fillna(0)
        res_q2["tasa_devolucion_pct"] = (res_q2["transacciones_devueltas"] / res_q2["total_transacciones"] * 100).round(2)
        res_q2["tasa_cancelacion_pct"] = (res_q2["transacciones_canceladas"] / res_q2["total_transacciones"] * 100).round(2)
    else:
        res_q2 = pd.DataFrame()

    # Consolidación final de la Consulta 3 (Ranking global tras acumulación)
    if acum_q3:
        c3 = pd.concat(acum_q3, ignore_index=True)
        col_dep = "departamento" if "departamento" in c3.columns else "departamento_tienda"
        agrup_c3 = c3.groupby([col_dep, "producto_id"], observed=True)["cantidad"].sum().reset_index()
        agrup_c3["ranking"] = agrup_c3.groupby(col_dep, observed=True)["cantidad"].rank(method="min", ascending=False).astype(int)
        res_q3 = agrup_c3[agrup_c3["ranking"] <= 10].sort_values([col_dep, "ranking"])
    else:
        res_q3 = pd.DataFrame()

    # Consolidación final de la Consulta 4
    if acum_q4_ventas:
        c4 = pd.concat(acum_q4_ventas, ignore_index=True)
        res_q4 = c4.groupby(["segmento", "anio_mes"], observed=True).agg(
            venta_total=("venta_total", "sum"),
            total_transacciones=("total_transacciones", "sum"),
        ).reset_index()
        res_q4["ticket_promedio"] = (res_q4["venta_total"] / res_q4["total_transacciones"]).round(2)

        # Incorporar cardinalidad de clientes únicos
        conteos = [{"segmento": s, "anio_mes": m, "clientes_unicos": len(st)} for (s, m), st in sets_clientes_unicos.items()]
        df_conteos = pd.DataFrame(conteos) if conteos else pd.DataFrame(columns=["segmento", "anio_mes", "clientes_unicos"])
        res_q4 = res_q4.merge(df_conteos, on=["segmento", "anio_mes"], how="left").fillna(0)
    else:
        res_q4 = pd.DataFrame()

    return {
        "pregunta_1_ventas_margen": res_q1,
        "pregunta_2_tasas_devolucion_cancelacion": res_q2,
        "pregunta_3_top10_productos": res_q3,
        "pregunta_4_ticket_clientes": res_q4,
    }
