"""
informe/preparar_secciones_1_5.py
=================================
Convierte la exportación en Markdown del Google Doc del equipo (secciones 1 a 5) en
`informe/secciones_1_5.md`, lista para ensamblar el PDF final:

  - extrae las imágenes incrustadas (base64) a `informe/img/`;
  - convierte los subtítulos que quedaron como texto plano (2.1, 4.1.1, 4.2.x...) en títulos;
  - pasa los fragmentos de código que estaban dentro de celdas de tabla a bloques de código;
  - renumera las tablas para que sean consecutivas (la tabla de técnicas de optimización de
    la sección 2 no tenía número: pasa a ser la Tabla 4 y las siguientes se corren una);
  - llena los marcadores que se pueden obtener del repositorio (volumen del dataset,
    ficha del equipo en evidencias/info_equipo.json, parámetros de sesion.py);
  - quita las notas internas ("✎ Figura sugerida", "Revisar antes de entregar") y la lista
    de referencias, que pasa a la sección 8.

Uso:
    python informe/preparar_secciones_1_5.py "Informe Proyecto Nota 3 - Secciones 1 y 3.md"
(En Google Docs: Archivo > Descargar > Markdown.)
"""

from __future__ import annotations

import base64
import json
import re
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
DIR_INFORME = RAIZ / "informe"
DIR_IMG = DIR_INFORME / "img"

# Volumen del dataset del equipo de pruebas (sección 5.2 del informe)
FILAS_MILLONES, GB_CSV, RAM_GB = "123,58", "11,53", "7,64"

CODIGO_4_1_1 = '''def iterar_transacciones_chunks(
    rutas: Sequence[Path],
    chunksize: int = 250_000,
    mitigado: bool = True,
    usecols: list[str] | None = None,
) -> Generator[pd.DataFrame, None, None]:
    """Generador por fragmentos (chunks) para mitigar el consumo de RAM.
    Aplica dtypes compactos y proyección selectiva de columnas."""
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
            yield chunk'''

CODIGO_4_1_2 = '''def limpiar_transacciones(df: pd.DataFrame, deduplicar: bool = True) -> pd.DataFrame:
    """Pipeline integral de limpieza sobre un lote transaccional."""
    if df.empty:
        return df
    df_limpio = df.copy()

    # 1. Deduplicación por transaccion_id
    if deduplicar and "transaccion_id" in df_limpio.columns:
        df_limpio = df_limpio.drop_duplicates(subset=["transaccion_id"], keep="first")

    # 2. Filtrado de cantidades y precios válidos
    if "cantidad" in df_limpio.columns:
        df_limpio = df_limpio[df_limpio["cantidad"] > 0]
    if "precio_unitario" in df_limpio.columns:
        df_limpio = df_limpio[df_limpio["precio_unitario"].notna() & (df_limpio["precio_unitario"] > 0)]

    # 3. Normalización de método de pago y parseo de fechas mixtas
    if "metodo_pago" in df_limpio.columns:
        df_limpio["metodo_pago"] = normalizar_metodo_pago(df_limpio["metodo_pago"])
    if "fecha_hora" in df_limpio.columns:
        dt, anio_mes = normalizar_fechas(df_limpio["fecha_hora"])
        df_limpio["fecha_hora"] = dt
        df_limpio["anio_mes"] = anio_mes
        df_limpio = df_limpio[df_limpio["fecha_hora"].notna()]

    return df_limpio'''

CODIGO_4_1_3 = '''def unir_clientes(df_transacciones: pd.DataFrame, df_clientes: pd.DataFrame) -> pd.DataFrame:
    """Realiza LEFT JOIN para retener compras sin fidelización (8 % con cliente_id nulo)."""
    cols = ["cliente_id", "segmento", "departamento", "genero"]
    cols_disponibles = [c for c in cols if c in df_clientes.columns]

    return df_transacciones.merge(
        df_clientes[cols_disponibles],
        on="cliente_id",
        how="left",  # Mandatorio para no perder facturación
        suffixes=("", "_cliente"),
    )'''

CODIGO_4_1_4 = '''def consulta_4_ticket_clientes_fidelizacion(df_unido: pd.DataFrame) -> pd.DataFrame:
    """Pregunta 4: Ticket promedio y clientes únicos por segmento y mes."""
    df_valido = df_unido[df_unido["estado"] == "COMPLETADA"].copy()
    df_valido = calcular_metricas_financieras(df_valido)
    df_valido["segmento"] = df_valido["segmento"].fillna("Sin fidelización")

    agrupado = (
        df_valido.groupby(["segmento", "anio_mes"], observed=True)
        .agg(
            ticket_promedio=("venta_neta", "mean"),
            clientes_unicos=("cliente_id", "nunique"),  # Métrica no distributiva
            total_transacciones=("transaccion_id", "count"),
            venta_total=("venta_neta", "sum"),
        )
        .reset_index()
    )
    agrupado["ticket_promedio"] = agrupado["ticket_promedio"].round(2)
    return agrupado.sort_values(["segmento", "anio_mes"]).reset_index(drop=True)'''


def ficha_equipo() -> dict[str, str]:
    info = json.loads((RAIZ / "evidencias" / "info_equipo.json").read_text(encoding="utf-8"))
    disco = (info.get("discos_fisicos") or [{}])[0]
    procesador = re.sub(r"\(R\)|\(TM\)", "", info["procesador"]).replace("  ", " ").strip()
    return {
        "so": info["sistema_operativo"],
        "cpu": f"{procesador}, {info['nucleos_fisicos']} núcleos y {info['hilos_logicos']} hilos",
        "ram": f"{info['ram_total_gb']:.2f}".replace(".", ","),
        "disco": (f"{disco.get('MediaType', 'SSD')} {disco.get('BusType', '')} {disco.get('FriendlyName', '')} "
                  f"de {disco.get('GB', '?')} GB, con {str(info['disco_libre_gb']).replace('.', ',')} GB libres "
                  f"en {info.get('disco_unidad_proyecto', 'C:')}").replace("  ", " ").replace("\\", "\\\\"),
    }


def reemplazar(texto: str, viejo: str, nuevo: str, obligatorio: bool = True) -> str:
    if viejo not in texto:
        if obligatorio:
            raise ValueError(f"No se encontró el texto esperado: {viejo[:70]!r}")
        return texto
    return texto.replace(viejo, nuevo)


def bloque_codigo_en_tabla(texto: str, inicio: str, codigo: str) -> str:
    """Reemplaza una tabla de una celda que empieza por `inicio` por un bloque de código."""
    patron = re.compile(r"^\| " + re.escape(inicio) + r".*?\n\| :-+ \|\n", re.M | re.S)
    nuevo, n = patron.subn(lambda _: f"```python\n{codigo}\n```\n", texto, count=1)
    if n != 1:
        raise ValueError(f"No se encontró la tabla de código que empieza por {inicio!r}")
    return nuevo


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    texto = Path(sys.argv[1]).read_text(encoding="utf-8")

    # 1. Imágenes incrustadas -> archivos
    DIR_IMG.mkdir(parents=True, exist_ok=True)
    for nombre, datos in re.findall(r"^\[(image\d+)\]: <data:image/png;base64,([^>]+)>", texto, re.M):
        (DIR_IMG / f"{nombre}.png").write_bytes(base64.b64decode(datos))
    texto = re.sub(r"^\[image\d+\]: <data:image/png;base64,[^>]+>\n?", "", texto, flags=re.M)
    texto = re.sub(r"\*?!\[\]\[(image\d+)\]\*?", r"![](img/\1.png)", texto)

    # 2. Portada (la genera ensamblar_pdf.py) y referencias (pasan a la sección 8)
    texto = texto[texto.index("# 1\\. Introducción"):]
    texto = texto[:texto.index("# **Referencias**")]

    # 3. Marcadores con datos del repositorio
    eq = ficha_equipo()
    texto = reemplazar(texto, r"procesa \[N\] millones de transacciones de venta, que ocupan \[X\] GB en formato CSV, "
                       r"en un portátil de \[RAM\] GB de memoria",
                       f"procesa {FILAS_MILLONES} millones de transacciones de venta, que ocupan {GB_CSV} GB en "
                       f"formato CSV, en un portátil de {RAM_GB} GB de memoria")
    texto = reemplazar(texto, r"se generaron \[X\] GB, que equivalen a \[N\] millones de filas y a \[X/RAM\] veces",
                       f"se generaron {GB_CSV} GB, que equivalen a {FILAS_MILLONES} millones de filas y a 1,51 veces")
    texto = reemplazar(texto, r"| \[Windows 11 Home 23H2, compilación …\] |", f"| {eq['so']} |")
    texto = reemplazar(texto, r"| \[Modelo\], \[n\] núcleos y \[n\] hilos |", f"| {eq['cpu']} |")
    texto = reemplazar(texto, r"| \[RAM\] GB |", f"| {eq['ram']} GB |")
    texto = reemplazar(texto, r"| \[SSD NVMe / HDD\] de \[capacidad\] GB, con \[n\] GB libres |", f"| {eq['disco']} |")
    texto = reemplazar(texto, r"| \[8g\] (50 %–60 % de la RAM) |", "| 4g (≈ 50 % de la RAM; lo calcula sesion.py) |")
    texto = reemplazar(texto, r"| \[64\] (2 a 4 veces los núcleos) |", "| 36 (3 × 12 hilos lógicos; lo calcula sesion.py) |")

    # 4. Notas internas
    texto = re.sub(r"\\\[✎ Figura sugerida:.*?\\\]\n", "", texto, flags=re.S)
    texto = re.sub(r"\\\[Revisar antes de entregar:.*?\\\]\n\n", "", texto, flags=re.S)

    # 5. Renumerar tablas: 4..9 -> 5..10 y la tabla sin número de §2 pasa a ser la 4
    for n in range(9, 3, -1):
        texto = re.sub(rf"Tabla {n}\b", f"Tabla {n + 1}", texto)
    texto = reemplazar(texto, r"(\[Tabla N\])", "(Tabla 4)")
    texto = reemplazar(texto, "| Técnica  |", "**Tabla 4.** *Técnicas de optimización de Spark aplicadas en el "
                       "proyecto*\n\n| Técnica  |")
    texto = re.sub(r"\nTabla N\. Técnicas de optimización de Spark aplicadas en el proyecto\n", "\n", texto)
    texto = reemplazar(texto, "| Muestra | Archivos | Filas |",
                       "**Tabla 10.** *Resumen de escalabilidad presentado en la sección 5*\n\n| Muestra | Archivos | Filas |")

    # 6. Subtítulos que quedaron como texto plano
    for sub in ["2.1 Big data", "2.2 Limitaciones de pandas", "2.3 Arquitectura de Apache Spark",
                "2.4 Modelo de ejecución: particiones, evaluación perezosa y DAG",
                "2.5 DataFrames, Spark SQL y optimización", "4.2 Versión con PySpark"]:
        texto = re.sub(rf"^{re.escape(sub)} *\n", f"## {sub}\n\n", texto, flags=re.M)
    texto = re.sub(r"^(4\.[12]\.\d [^\n]+?) *\n", r"### \1\n\n", texto, flags=re.M)
    texto = re.sub(r"^## \*\*(.+?)\*\*$", r"## \1", texto, flags=re.M)

    # 7. Código dentro de tablas -> bloques de código
    texto = bloque_codigo_en_tabla(texto, r"def iterar\_transacciones\_chunks(", CODIGO_4_1_1)
    texto = bloque_codigo_en_tabla(texto, r"def limpiar\_transacciones(", CODIGO_4_1_2)
    texto = bloque_codigo_en_tabla(texto, r"def unir\_clientes(", CODIGO_4_1_3)
    texto = bloque_codigo_en_tabla(texto, r"def consulta\_4\_ticket\_clientes\_fidelizacion(", CODIGO_4_1_4)

    # 8. Saltos de línea forzados a mitad de frase y viñetas pegadas
    texto = re.sub(r"^[ \t]+$", "", texto, flags=re.M)              # líneas con solo espacios
    texto = re.sub(r"  \n(?=[a-záéíóúñ(`])", " ", texto)           # salto forzado a mitad de frase
    texto = re.sub(r"  \n(?=[A-ZÁÉÍÓÚÑ¿])", "\n\n", texto)            # salto forzado entre frases
    texto = re.sub(r"en\n\n  memoria o en disco", "en memoria o en disco", texto)
    texto = re.sub(r"^• ", "- ", texto, flags=re.M)
    texto = re.sub(r"  \n ?• ", "\n- ", texto)
    texto = re.sub(r"  \n (\d)\\?\. ", r"\n\1. ", texto)
    texto = re.sub(r"(Análisis del impacto en el sistema:) *\n ?1\\?\. ", r"\1\n\n1. ", texto)
    texto = re.sub(r"^1\\\. Inviabilidad física", "1. Inviabilidad física", texto, flags=re.M)
    texto = reemplazar(texto, r"\-**Ejecución inmediata.**", "**Ejecución inmediata.**")
    texto = reemplazar(texto, "*  **Gestor de recursos:** (*cluster manager*):",
                       "* **Gestor de recursos** (*cluster manager*):")
    texto = texto.replace("sin *(shuffle).*", "sin *shuffle*.")
    texto = reemplazar(texto, "**DAG, etapas y** *(shuffle)***:**", "**DAG, etapas y *shuffle*:**")
    texto = texto.replace(r"*\*shuffles\**", "*shuffles*")
    # Términos en cursiva escritos como "(*x)*", "*(x)*" o "(*x*)"
    texto = re.sub(r"\(\*([^()*\n]+?)\)\*", r"(*\1*)", texto)
    texto = re.sub(r"\*\(([^()*\n]+?)\)\*", r"(*\1*)", texto)
    # Lista de módulos de §4.1 con el nombre del archivo en negrita
    texto = re.sub(r"^- (\w+\.py): ", r"- **\1:** ", texto, flags=re.M)
    texto = re.sub(r" *\n +(La lectura por fragmentos)", r"\n\n\1", texto)
    texto = re.sub(r"  $", "", texto, flags=re.M)                      # saltos forzados sobrantes
    texto = re.sub(r"\n{3,}", "\n\n", texto)

    pendientes = re.findall(r"\\\[[^\]\n]{0,40}\\\]", texto)
    salida = DIR_INFORME / "secciones_1_5.md"
    salida.write_text(texto.strip() + "\n", encoding="utf-8")
    print(f"Escrito {salida} ({len(texto):,} caracteres) e imágenes en {DIR_IMG}")
    if pendientes:
        print("Marcadores que siguen en el texto:", sorted(set(pendientes)))


if __name__ == "__main__":
    main()
