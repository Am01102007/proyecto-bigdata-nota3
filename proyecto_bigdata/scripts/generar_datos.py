"""
generar_datos.py
================
Genera el conjunto de datos sintético del proyecto: ventas de una cadena
retail omnicanal en Colombia (tiendas físicas + web + app), 2023-2025.

Produce:
    <salida>/dimensiones/clientes.csv
    <salida>/dimensiones/productos.csv
    <salida>/dimensiones/tiendas.csv
    <salida>/transacciones/part-00000.csv ... part-NNNNN.csv   (tabla de hechos)
    <salida>/manifiesto.json                                  (metadatos de la generación)

La tabla de hechos se divide en N partes del mismo tamaño. Así las muestras del
1 %, 10 %, 50 % y 100 % son simplemente las primeras 1, 10, 50 y 100 partes
(ver comun/muestras.py), idénticas para la versión pandas y la versión Spark.

Los datos incluyen "suciedad" intencional y realista (nulos, duplicados,
formatos de fecha mezclados, cantidades negativas, texto inconsistente) para
que la etapa de limpieza tenga sentido.

Es 100 % reproducible: misma semilla + mismos parámetros = mismos archivos.

Uso (desde la raíz del proyecto):
    python scripts/generar_datos.py                      # tamaño automático = 1.5 x RAM del equipo
    python scripts/generar_datos.py --tamano-gb 24       # tamaño fijo
    python scripts/generar_datos.py --tamano-gb 0.5 --partes 10   # prueba rápida
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv

try:
    import psutil
except ImportError:  # psutil solo se usa para el tamaño automático
    psutil = None

# ---------------------------------------------------------------------------
# Catálogos de referencia
# ---------------------------------------------------------------------------
# (ciudad, departamento, región, peso aproximado por población)
CIUDADES = [
    ("Bogotá", "Bogotá D.C.", "Andina", 7.9),
    ("Medellín", "Antioquia", "Andina", 2.6),
    ("Cali", "Valle del Cauca", "Pacífica", 2.3),
    ("Barranquilla", "Atlántico", "Caribe", 1.3),
    ("Cartagena", "Bolívar", "Caribe", 1.0),
    ("Bucaramanga", "Santander", "Andina", 0.6),
    ("Cúcuta", "Norte de Santander", "Andina", 0.8),
    ("Pereira", "Risaralda", "Andina", 0.5),
    ("Manizales", "Caldas", "Andina", 0.45),
    ("Ibagué", "Tolima", "Andina", 0.55),
    ("Santa Marta", "Magdalena", "Caribe", 0.55),
    ("Villavicencio", "Meta", "Orinoquía", 0.55),
    ("Pasto", "Nariño", "Pacífica", 0.4),
    ("Montería", "Córdoba", "Caribe", 0.5),
    ("Neiva", "Huila", "Andina", 0.37),
    ("Armenia", "Quindío", "Andina", 0.3),
    ("Popayán", "Cauca", "Pacífica", 0.32),
    ("Valledupar", "Cesar", "Caribe", 0.53),
    ("Sincelejo", "Sucre", "Caribe", 0.3),
    ("Tunja", "Boyacá", "Andina", 0.2),
    ("Riohacha", "La Guajira", "Caribe", 0.2),
    ("Quibdó", "Chocó", "Pacífica", 0.13),
    ("Florencia", "Caquetá", "Amazonía", 0.18),
    ("Yopal", "Casanare", "Orinoquía", 0.17),
    ("Leticia", "Amazonas", "Amazonía", 0.05),
]

# (categoría, [subcategorías], mediana precio COP, sigma lognormal, margen medio)
CATEGORIAS = [
    ("Alimentos", ["Granos", "Lácteos", "Carnes", "Panadería", "Enlatados", "Snacks"], 9_000, 0.6, 0.18),
    ("Bebidas", ["Gaseosas", "Jugos", "Agua", "Cerveza", "Licores", "Café"], 6_500, 0.7, 0.22),
    ("Aseo hogar", ["Detergentes", "Limpiadores", "Papel", "Desinfectantes"], 14_000, 0.5, 0.25),
    ("Cuidado personal", ["Shampoo", "Jabón", "Desodorante", "Cuidado oral", "Maquillaje"], 18_000, 0.6, 0.30),
    ("Electrónica", ["Celulares", "Televisores", "Audio", "Computadores", "Accesorios"], 450_000, 1.1, 0.12),
    ("Hogar", ["Cocina", "Decoración", "Muebles", "Textil hogar"], 85_000, 0.9, 0.35),
    ("Ropa", ["Hombre", "Mujer", "Niños", "Calzado", "Deportiva"], 70_000, 0.6, 0.45),
    ("Juguetería", ["Didácticos", "Muñecas", "Vehículos", "Juegos de mesa"], 60_000, 0.7, 0.40),
    ("Mascotas", ["Alimento perro", "Alimento gato", "Accesorios mascota"], 35_000, 0.7, 0.28),
    ("Farmacia", ["Analgésicos", "Vitaminas", "Primeros auxilios"], 16_000, 0.6, 0.20),
]
PESO_CATEGORIA = np.array([30, 18, 10, 10, 4, 7, 8, 3, 5, 5], dtype=float)

MARCAS = [f"Marca{chr(65 + i)}{j}" for i in range(26) for j in range(1, 13)]  # 312 marcas
TIPOS_TIENDA = ["Hipermercado", "Supermercado", "Express", "Centro de distribución e-commerce"]
SEGMENTOS = ["Bronce", "Plata", "Oro", "Platino"]

CANALES = ["Tienda física", "Web", "App"]
PESO_CANAL = [0.70, 0.18, 0.12]

METODOS_PAGO = ["Efectivo", "Tarjeta débito", "Tarjeta crédito", "Nequi", "Daviplata", "PSE"]
PESO_PAGO = [0.30, 0.24, 0.22, 0.12, 0.06, 0.06]
# Variantes "sucias" del mismo valor (mayúsculas, espacios, sin tilde)
VARIANTES_PAGO = [
    "EFECTIVO", "efectivo ", " Efectivo", "TARJETA DEBITO", "tarjeta débito", "Tarjeta Debito",
    "TARJETA CREDITO", "tarjeta crédito", "Tarjeta Credito", "NEQUI", "nequi", "DAVIPLATA", "pse",
]

ESTADOS = ["COMPLETADA", "DEVUELTA", "CANCELADA"]
PESO_ESTADO = [0.93, 0.04, 0.03]

FECHA_INICIO = np.datetime64("2023-01-01")
FECHA_FIN = np.datetime64("2026-01-01")  # exclusivo

# Proporciones de "suciedad"
P_CLIENTE_NULO = 0.08       # compras anónimas (sin tarjeta de fidelización)
P_FECHA_FORMATO_ALT = 0.005  # fecha en formato dd/mm/aaaa HH:MM
P_FECHA_NULA = 0.0005
P_PRECIO_NULO = 0.002
P_CANTIDAD_INVALIDA = 0.003  # cantidad 0 o negativa
P_PAGO_SUCIO = 0.03
P_DUPLICADO = 0.003          # filas repetidas (reenvíos del sistema POS)

COLUMNAS_HECHOS = [
    "transaccion_id", "fecha_hora", "cliente_id", "producto_id", "tienda_id", "canal",
    "cantidad", "precio_unitario", "descuento_pct", "metodo_pago", "estado",
]


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
def _cadenas(valores: list[str], indices: np.ndarray) -> pa.Array:
    """Convierte índices enteros a un arreglo Arrow de texto (rápido)."""
    return pa.array(valores, type=pa.string()).take(pa.array(indices))


def _escribir_csv(tabla: pa.Table, ruta: Path) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    opciones = pacsv.WriteOptions(include_header=True, quoting_style="needed")
    pacsv.write_csv(tabla, str(ruta), write_options=opciones)


def _pesos_ciudades() -> np.ndarray:
    w = np.array([c[3] for c in CIUDADES], dtype=float)
    return w / w.sum()


def _pesos_zipf(n: int, s: float, rng: np.random.Generator) -> np.ndarray:
    """Popularidad tipo ley de potencias (pocos productos venden mucho)."""
    w = 1.0 / np.arange(1, n + 1) ** s
    rng.shuffle(w)  # la popularidad no depende del id
    return w / w.sum()


# ---------------------------------------------------------------------------
# Dimensiones
# ---------------------------------------------------------------------------
def generar_tiendas(n: int, rng: np.random.Generator) -> pa.Table:
    ciudad_idx = rng.choice(len(CIUDADES), size=n, p=_pesos_ciudades())
    tipo_idx = rng.choice(len(TIPOS_TIENDA), size=n, p=[0.12, 0.45, 0.38, 0.05])
    apertura = FECHA_INICIO - rng.integers(0, 365 * 25, size=n).astype("timedelta64[D]")
    return pa.table({
        "tienda_id": np.arange(1, n + 1, dtype=np.int32),
        "nombre_tienda": [f"Tienda {CIUDADES[c][0]} {i:04d}" for i, c in enumerate(ciudad_idx, 1)],
        "tipo_tienda": _cadenas(TIPOS_TIENDA, tipo_idx),
        "ciudad": _cadenas([c[0] for c in CIUDADES], ciudad_idx),
        "departamento": _cadenas([c[1] for c in CIUDADES], ciudad_idx),
        "region": _cadenas([c[2] for c in CIUDADES], ciudad_idx),
        "fecha_apertura": pa.array(apertura.astype("datetime64[D]")),
        "area_m2": rng.choice([3500, 1500, 250, 8000], size=n)[tipo_idx]
        + rng.integers(-100, 100, size=n),
    })


def generar_productos(n: int, rng: np.random.Generator) -> pa.Table:
    cat_idx = rng.choice(len(CATEGORIAS), size=n, p=PESO_CATEGORIA / PESO_CATEGORIA.sum())
    sub, precio, margen = [], np.empty(n), np.empty(n)
    for i, c in enumerate(cat_idx):
        nombre, subs, med, sig, mrg = CATEGORIAS[c]
        sub.append(subs[rng.integers(len(subs))])
        precio[i] = med * rng.lognormal(0.0, sig)
        margen[i] = np.clip(rng.normal(mrg, 0.06), 0.03, 0.75)
    precio = np.round(np.maximum(precio, 500) / 50) * 50  # precios "redondos" en COP
    costo = np.round(precio * (1 - margen), 2)
    marca_idx = rng.integers(0, len(MARCAS), size=n)
    return pa.table({
        "producto_id": np.arange(1, n + 1, dtype=np.int32),
        "nombre_producto": [f"{sub[i]} {MARCAS[marca_idx[i]]} ref-{i + 1:06d}" for i in range(n)],
        "categoria": _cadenas([c[0] for c in CATEGORIAS], cat_idx),
        "subcategoria": pa.array(sub),
        "marca": _cadenas(MARCAS, marca_idx),
        "precio_lista": precio,
        "costo_unitario": costo,
    })


def generar_clientes(n: int, rng: np.random.Generator) -> pa.Table:
    ciudad_idx = rng.choice(len(CIUDADES), size=n, p=_pesos_ciudades())
    edad = np.clip(rng.normal(39, 13, size=n), 18, 90).astype(np.int16)
    edad_arr = pa.array(edad, mask=rng.random(n) < 0.04)  # 4 % sin edad registrada
    registro = FECHA_INICIO - rng.integers(0, 365 * 10, size=n).astype("timedelta64[D]")
    return pa.table({
        "cliente_id": np.arange(1, n + 1, dtype=np.int32),
        "ciudad": _cadenas([c[0] for c in CIUDADES], ciudad_idx),
        "departamento": _cadenas([c[1] for c in CIUDADES], ciudad_idx),
        "segmento": _cadenas(SEGMENTOS, rng.choice(4, size=n, p=[0.55, 0.28, 0.13, 0.04])),
        "genero": _cadenas(["F", "M", "O"], rng.choice(3, size=n, p=[0.52, 0.46, 0.02])),
        "edad": edad_arr,
        "fecha_registro": pa.array(registro.astype("datetime64[D]")),
        "acepta_marketing": rng.random(n) < 0.6,
    })


# ---------------------------------------------------------------------------
# Tabla de hechos
# ---------------------------------------------------------------------------
def _pesos_dias() -> tuple[np.ndarray, np.ndarray]:
    """Estacionalidad: más ventas en fin de semana, quincenas y diciembre."""
    dias = np.arange(FECHA_INICIO, FECHA_FIN, dtype="datetime64[D]")
    dow = (dias.astype("int64") + 3) % 7  # 0 = lunes
    mes = dias.astype("datetime64[M]").astype("int64") % 12 + 1
    dia_mes = (dias - dias.astype("datetime64[M]")).astype("int64") + 1
    w = np.ones(len(dias))
    w *= np.where(dow >= 4, 1.35, 1.0)                      # vie-sáb-dom
    w *= np.where(np.isin(dia_mes, [15, 16, 30, 31, 1]), 1.25, 1.0)  # quincena
    w *= np.where(mes == 12, 1.6, 1.0)                       # navidad
    w *= np.where(mes == 11, 1.15, 1.0)                      # black friday
    # crecimiento anual ~8 %
    w *= 1 + 0.08 * (dias - FECHA_INICIO).astype("int64") / 365
    return dias, w / w.sum()


def _pesos_hora() -> np.ndarray:
    h = np.arange(24)
    w = np.exp(-((h - 18) ** 2) / 18) + 0.6 * np.exp(-((h - 12) ** 2) / 8)
    w[(h < 6) | (h > 22)] *= 0.05
    return w / w.sum()


def generar_parte(
    parte: int, filas: int, semilla: int, n_clientes: int, n_tiendas: int,
    precios: np.ndarray, pesos_prod: np.ndarray, pesos_tienda: np.ndarray, ruta: str,
) -> dict:
    """Genera y escribe una parte de la tabla de hechos. Se ejecuta en un proceso hijo."""
    t0 = time.perf_counter()
    rng = np.random.default_rng([semilla, parte])
    n = filas

    # Identificador global único: las partes no se solapan
    tx_id = np.arange(parte * filas, (parte + 1) * filas, dtype=np.int64) + 1_000_000_000

    # Fecha y hora con estacionalidad
    dias, p_dias = _pesos_dias()
    dia = dias[rng.choice(len(dias), size=n, p=p_dias)]
    seg = rng.choice(24, size=n, p=_pesos_hora()) * 3600 + rng.integers(0, 3600, size=n)
    ts = dia.astype("datetime64[s]") + seg.astype("timedelta64[s]")
    ts_arr = pa.array(ts)
    fecha_std = pc.strftime(ts_arr, format="%Y-%m-%d %H:%M:%S")
    fecha_alt = pc.strftime(ts_arr, format="%d/%m/%Y %H:%M")
    fecha = pc.if_else(pa.array(rng.random(n) < P_FECHA_FORMATO_ALT), fecha_alt, fecha_std)
    fecha = pc.if_else(pa.array(rng.random(n) < P_FECHA_NULA), pa.scalar(None, pa.string()), fecha)

    # Claves foráneas
    cliente = rng.integers(1, n_clientes + 1, size=n, dtype=np.int32)
    cliente_arr = pa.array(cliente, mask=rng.random(n) < P_CLIENTE_NULO)
    producto = rng.choice(len(pesos_prod), size=n, p=pesos_prod).astype(np.int32) + 1
    tienda = rng.choice(n_tiendas, size=n, p=pesos_tienda).astype(np.int32) + 1

    canal_idx = rng.choice(3, size=n, p=PESO_CANAL)

    # Cantidad (con algunos valores inválidos)
    cantidad = (1 + rng.poisson(0.8, size=n)).astype(np.int32)
    malas = rng.random(n) < P_CANTIDAD_INVALIDA
    cantidad[malas] = rng.integers(-3, 1, size=int(malas.sum()))

    # Precio: precio de lista ± 3 % de variación, con nulos
    precio = np.round(precios[producto - 1] * rng.uniform(0.97, 1.03, size=n), 0)
    precio_arr = pa.array(precio, mask=rng.random(n) < P_PRECIO_NULO)

    descuento = np.where(
        rng.random(n) < 0.25, rng.choice([5, 10, 15, 20, 30, 50], size=n), 0
    ).astype(np.int8)

    # Método de pago con variantes sucias
    pago = _cadenas(METODOS_PAGO, rng.choice(len(METODOS_PAGO), size=n, p=PESO_PAGO))
    pago_sucio = _cadenas(VARIANTES_PAGO, rng.integers(0, len(VARIANTES_PAGO), size=n))
    pago = pc.if_else(pa.array(rng.random(n) < P_PAGO_SUCIO), pago_sucio, pago)

    estado = _cadenas(ESTADOS, rng.choice(3, size=n, p=PESO_ESTADO))

    tabla = pa.table({
        "transaccion_id": tx_id,
        "fecha_hora": fecha,
        "cliente_id": cliente_arr,
        "producto_id": producto,
        "tienda_id": tienda,
        "canal": _cadenas(CANALES, canal_idx),
        "cantidad": cantidad,
        "precio_unitario": precio_arr,
        "descuento_pct": descuento,
        "metodo_pago": pago,
        "estado": estado,
    })

    # Duplicados exactos (reenvíos del POS), mezclados en posiciones aleatorias
    n_dup = int(n * P_DUPLICADO)
    if n_dup:
        dup = tabla.take(pa.array(rng.choice(n, size=n_dup, replace=False)))
        tabla = pa.concat_tables([tabla, dup])
        tabla = tabla.take(pa.array(rng.permutation(tabla.num_rows)))

    _escribir_csv(tabla, Path(ruta))
    return {
        "parte": parte,
        "filas": tabla.num_rows,
        "bytes": os.path.getsize(ruta),
        "segundos": round(time.perf_counter() - t0, 2),
    }


# ---------------------------------------------------------------------------
# Programa principal
# ---------------------------------------------------------------------------
def ram_total_gb() -> float | None:
    return psutil.virtual_memory().total / 1024**3 if psutil else None


def calibrar_bytes_por_fila(args, precios, pesos_prod, pesos_tienda, tmp: Path) -> float:
    """Genera una parte pequeña para medir cuántos bytes ocupa cada fila en CSV."""
    ruta = tmp / "calibracion.csv"
    info = generar_parte(0, 200_000, args.semilla, args.clientes, args.tiendas,
                         precios, pesos_prod, pesos_tienda, str(ruta))
    ruta.unlink()
    return info["bytes"] / info["filas"]


def main() -> None:
    ap = argparse.ArgumentParser(description="Generador del dataset sintético de ventas retail.")
    ap.add_argument("--salida", default="datos/raw", help="Carpeta de salida (default: datos/raw)")
    ap.add_argument("--tamano-gb", type=float, default=None,
                    help="Tamaño objetivo de la tabla de hechos en GB. Default: 1.5 x RAM del equipo.")
    ap.add_argument("--factor-ram", type=float, default=1.5,
                    help="Multiplicador de la RAM cuando no se indica --tamano-gb (default 1.5).")
    ap.add_argument("--partes", type=int, default=100, help="Número de archivos de hechos (default 100).")
    ap.add_argument("--clientes", type=int, default=5_000_000)
    ap.add_argument("--productos", type=int, default=60_000)
    ap.add_argument("--tiendas", type=int, default=600)
    ap.add_argument("--semilla", type=int, default=2026)
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1),
                    help="Procesos en paralelo (default: núcleos - 1).")
    ap.add_argument("--forzar", action="store_true", help="Sobrescribe la carpeta de salida si existe.")
    args = ap.parse_args()

    salida = Path(args.salida)
    if salida.exists() and any(salida.iterdir()):
        if not args.forzar:
            sys.exit(f"La carpeta {salida} ya tiene datos. Use --forzar para regenerarlos.")
        shutil.rmtree(salida)
    (salida / "transacciones").mkdir(parents=True, exist_ok=True)

    # Tamaño objetivo
    ram = ram_total_gb()
    if args.tamano_gb is None:
        if ram is None:
            sys.exit("No se pudo leer la RAM (instale psutil) o indique --tamano-gb.")
        args.tamano_gb = round(ram * args.factor_ram, 1)
        print(f"RAM detectada: {ram:.1f} GB -> tamaño objetivo {args.tamano_gb} GB "
              f"({args.factor_ram} x RAM)")

    libre = shutil.disk_usage(salida).free / 1024**3
    necesario = args.tamano_gb * 1.6  # CSV + salidas Parquet + temporales de Spark
    if libre < necesario:
        sys.exit(f"Espacio libre insuficiente: {libre:.1f} GB disponibles, "
                 f"se recomiendan {necesario:.1f} GB (datos + resultados + temporales).")

    t_inicio = time.perf_counter()
    rng = np.random.default_rng(args.semilla)

    # 1) Dimensiones
    print("Generando dimensiones...")
    tiendas = generar_tiendas(args.tiendas, rng)
    productos = generar_productos(args.productos, rng)
    clientes = generar_clientes(args.clientes, rng)
    dim = salida / "dimensiones"
    _escribir_csv(tiendas, dim / "tiendas.csv")
    _escribir_csv(productos, dim / "productos.csv")
    _escribir_csv(clientes, dim / "clientes.csv")

    precios = productos.column("precio_lista").to_numpy()
    pesos_prod = _pesos_zipf(args.productos, 0.9, rng)
    area = tiendas.column("area_m2").to_numpy().astype(float)
    pesos_tienda = area / area.sum()

    # 2) Calibración del tamaño
    bpf = calibrar_bytes_por_fila(args, precios, pesos_prod, pesos_tienda, salida)
    filas_totales = int(args.tamano_gb * 1024**3 / bpf)
    filas_parte = int(np.ceil(filas_totales / args.partes))
    print(f"~{bpf:.0f} bytes/fila -> {filas_totales:,} filas en {args.partes} partes "
          f"de {filas_parte:,} filas (+{P_DUPLICADO:.1%} duplicados)")

    # 3) Tabla de hechos en paralelo
    resultados = []
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futuros = [
            ex.submit(generar_parte, i, filas_parte, args.semilla, args.clientes, args.tiendas,
                      precios, pesos_prod, pesos_tienda,
                      str(salida / "transacciones" / f"part-{i:05d}.csv"))
            for i in range(args.partes)
        ]
        for k, f in enumerate(as_completed(futuros), 1):
            r = f.result()
            resultados.append(r)
            gb = sum(x["bytes"] for x in resultados) / 1024**3
            print(f"  [{k:>4}/{args.partes}] part-{r['parte']:05d}  {r['filas']:,} filas  "
                  f"{r['segundos']} s   acumulado {gb:.2f} GB", flush=True)

    resultados.sort(key=lambda r: r["parte"])
    total_bytes = sum(r["bytes"] for r in resultados)
    total_filas = sum(r["filas"] for r in resultados)
    duracion = time.perf_counter() - t_inicio

    manifiesto = {
        "descripcion": "Ventas retail omnicanal sintéticas - Colombia 2023-2025",
        "semilla": args.semilla,
        "parametros": {k: v for k, v in vars(args).items() if k not in ("salida", "forzar")},
        "ram_equipo_gb": round(ram, 2) if ram else None,
        "transacciones": {
            "partes": args.partes,
            "filas_totales": total_filas,
            "bytes_totales": total_bytes,
            "gb_totales": round(total_bytes / 1024**3, 3),
            "detalle_partes": resultados,
        },
        "dimensiones": {
            "clientes": {"filas": clientes.num_rows, "bytes": os.path.getsize(dim / "clientes.csv")},
            "productos": {"filas": productos.num_rows, "bytes": os.path.getsize(dim / "productos.csv")},
            "tiendas": {"filas": tiendas.num_rows, "bytes": os.path.getsize(dim / "tiendas.csv")},
        },
        "duracion_segundos": round(duracion, 1),
    }
    (salida / "manifiesto.json").write_text(json.dumps(manifiesto, indent=2, ensure_ascii=False),
                                            encoding="utf-8")

    print(f"\nListo en {duracion / 60:.1f} min: {total_filas:,} transacciones, "
          f"{total_bytes / 1024**3:.2f} GB en {salida / 'transacciones'}")
    if ram:
        print(f"Relación tamaño/RAM: {total_bytes / 1024**3 / ram:.2f}x")


if __name__ == "__main__":
    main()
