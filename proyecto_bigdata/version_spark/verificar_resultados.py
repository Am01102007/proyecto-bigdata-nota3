"""
version_spark/verificar_resultados.py
=====================================
Comprueba la coherencia interna de los resultados de la última ejecución de Spark.

No compara con pandas (eso lo hace validar_equivalencia.py): verifica que las cuatro
consultas cuadran entre sí y con la base limpia, y que el Parquet particionado
permite leer solo las carpetas necesarias (partition pruning).

Requiere haber ejecutado antes el pipeline con --guardar-base:

    python version_spark/main.py --muestra 100 --guardar-base
    python version_spark/verificar_resultados.py

Termina con código 1 si alguna comprobación falla.
"""

from __future__ import annotations

import contextlib
import io
import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

from pyspark.sql import functions as F  # noqa: E402

from version_spark.escritura import DIR_RESULTADOS_SPARK  # noqa: E402
from version_spark.sesion import crear_sesion  # noqa: E402


def main() -> None:
    resumen_json = DIR_RESULTADOS_SPARK / "resumen_ejecucion.json"
    dir_base = DIR_RESULTADOS_SPARK / "ventas_limpias"
    if not resumen_json.exists() or not dir_base.exists():
        print("Falta ejecutar antes: python version_spark/main.py --muestra N --guardar-base")
        sys.exit(1)
    resumen = json.loads(resumen_json.read_text(encoding="utf-8"))

    spark = crear_sesion("verificar-resultados")
    fallas = 0

    def comprobar(descripcion: str, condicion: bool) -> None:
        nonlocal fallas
        fallas += not condicion
        print(f"[{'OK' if condicion else 'FALLA'}] {descripcion}")

    try:
        base = spark.read.parquet(str(dir_base))
        q1, q2, q3, q4 = (spark.read.parquet(str(DIR_RESULTADOS_SPARK / n)) for n in (
            "pregunta_1_ventas_margen", "pregunta_2_tasas_devolucion_cancelacion",
            "pregunta_3_top10_productos", "pregunta_4_ticket_clientes"))

        n = base.count()
        print(f"Muestra: {resumen['muestra_pct']} % | Filas de la base limpia: {n:,}\n")
        if resumen.get("filas_base") is not None:
            comprobar("La base en Parquet tiene las mismas filas que contó el pipeline",
                      n == resumen["filas_base"])
        meses = base.select("anio", "mes").distinct().count()
        carpetas = len(list(dir_base.glob("anio=*/mes=*")))
        comprobar(f"Una carpeta por año/mes ({carpetas} carpetas)", carpetas == meses)

        comprobar("Q1: unidades = unidades no canceladas de la base",
                  q1.agg(F.sum("unidades_vendidas")).first()[0]
                  == base.where("estado != 'CANCELADA'").agg(F.sum("cantidad")).first()[0])
        comprobar("Q2: suma de transacciones = filas de la base",
                  q2.agg(F.sum("total_transacciones")).first()[0] == n)
        comprobar("Q3: ranking entre 1 y 10",
                  tuple(q3.agg(F.min("ranking"), F.max("ranking")).first()) == (1, 10))
        comprobar("Q3: al menos 10 productos por departamento (más solo si hay empates)",
                  q3.groupBy("departamento").count().where("count < 10").count() == 0)
        comprobar("Q4: transacciones = transacciones COMPLETADA de la base",
                  q4.agg(F.sum("total_transacciones")).first()[0]
                  == base.where("estado = 'COMPLETADA'").count())
        comprobar("Q4: incluye las compras sin cliente ('Sin fidelización')",
                  q4.where("segmento = 'Sin fidelización'").count() > 0)
        comprobar("Q4: clientes únicos <= transacciones en cada grupo",
                  q4.where("clientes_unicos > total_transacciones").count() == 0)

        anio, mes = base.select("anio", "mes").orderBy("anio", "mes").first()
        filtrado = base.where((F.col("anio") == anio) & (F.col("mes") == mes))
        plan = io.StringIO()
        with contextlib.redirect_stdout(plan):
            filtrado.explain()
        comprobar(f"Partition pruning: filtrar {anio}-{mes:02d} lee solo su carpeta "
                  "(PartitionFilters en el plan)", "PartitionFilters: [isnotnull(anio" in plan.getvalue())
    finally:
        spark.stop()

    print(f"\n{'Todas las comprobaciones correctas.' if not fallas else f'{fallas} comprobación(es) fallida(s).'}")
    sys.exit(1 if fallas else 0)


if __name__ == "__main__":
    main()
