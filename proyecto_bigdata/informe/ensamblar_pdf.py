"""
informe/ensamblar_pdf.py
========================
Arma el PDF final del informe: portada + secciones 1-5 (secciones_1_5.md) + secciones
6-8 (secciones_6_7_8.md, generadas desde el benchmark).

Requisitos: pandoc (https://pandoc.org) y Google Chrome o Microsoft Edge (se usa su
impresión a PDF en modo headless; Edge viene instalado en Windows).

Uso:
    python informe/preparar_secciones_1_5.py "<exportación .md del Google Doc>"   # si cambió §1-5
    python benchmark/graficos.py && python benchmark/complejidad_codigo.py
    python informe/generar_secciones.py
    python informe/ensamblar_pdf.py                 # -> informe/Informe_Proyecto_Nota_3.pdf

Los datos de la portada están en informe/portada.json. Un valor entre corchetes, como
"[Nombre del docente]", se resalta en amarillo en el PDF para que no se entregue sin llenar.
"""

from __future__ import annotations

import html
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
DIR = RAIZ / "informe"
SALIDA_PDF = DIR / "Informe_Proyecto_Nota_3.pdf"
ENCABEZADO = "Proyecto Nota 3 · Big Data con Python y PySpark"

CSS = """
@page { size: Letter; margin: 2.3cm 2.4cm 2.2cm 2.4cm;
  @top-right { content: "%(encabezado)s"; font: 8.5pt 'Open Sans', Arial, sans-serif; color: #2a8c8c;
               border-bottom: 0.6pt solid #b9dcdc; vertical-align: bottom; padding-bottom: 4pt; width: 100%%;
               text-align: right; }
  @bottom-center { content: counter(page); font: 9pt Gelasio, Georgia, serif; color: #2a8c8c; } }
@page portada { @top-right { content: none; border: none; } @bottom-center { content: none; } }
html { font-family: Gelasio, Georgia, 'Times New Roman', serif; font-size: 11pt; line-height: 1.45; color: #1b1b1b; }
body { margin: 0; }
p, li { text-align: justify; hyphens: auto; orphans: 3; widows: 3; }
h1, h2, h3 { font-family: Gelasio, Georgia, serif; color: #1f7f7f; line-height: 1.2; break-after: avoid; }
h1 { font-size: 21pt; margin: 0 0 10pt; break-before: page; }
h2 { font-size: 15pt; margin: 18pt 0 6pt; }
h3 { font-size: 12.5pt; margin: 14pt 0 4pt; }
h1 + h2 { margin-top: 6pt; }
img { max-width: 100%%; display: block; margin: 8pt auto 4pt; break-inside: avoid; }
p:has(> img) { text-align: center; break-inside: avoid; break-after: avoid; }
table { border-collapse: collapse; width: 100%%; margin: 6pt 0 12pt; font-size: 9pt; line-height: 1.3;
        break-inside: auto; }
thead { display: table-header-group; }
tr { break-inside: avoid; }
th { background: #1f7f7f; color: #fff; font-weight: bold; text-align: left; padding: 4pt 5pt; vertical-align: top; }
td { border-bottom: 0.5pt solid #c9d6d6; padding: 3.5pt 5pt; vertical-align: top; text-align: left;
     hyphens: manual; overflow-wrap: normal; word-break: normal; }
tbody tr:nth-child(even) td { background: #f3f8f8; }
p:has(+ table) { break-after: avoid; }
code { font-family: 'DejaVu Sans Mono', Consolas, monospace; font-size: 8.6pt; color: #24292e; }
p code, li code, td code { background: #f1f3f3; padding: 0 2pt; border-radius: 2pt; }
pre { background: #f6f8f8; border: 0.6pt solid #d5dede; border-left: 3pt solid #2a8c8c; padding: 7pt 9pt;
      font-size: 8.4pt; line-height: 1.35; white-space: pre-wrap; break-inside: avoid; margin: 6pt 0 10pt; }
pre code { background: none; padding: 0; font-size: 7.9pt; }
pre > code.sourceCode > span { text-indent: 0 !important; padding-left: 0 !important; display: inline !important; }
pre.sourceCode { overflow: visible; }
blockquote { border-left: 3pt solid #b9dcdc; margin: 8pt 0; padding: 2pt 10pt; color: #444; }
strong { color: #111; }
.referencias p { text-align: left; padding-left: 1.27cm; text-indent: -1.27cm; margin: 0 0 7pt; hyphens: none; }
.marcador, .pendiente { background: #ffe14d; }
section.portada { page: portada; height: 23.2cm; display: flex; flex-direction: column; justify-content: space-between;
                  text-align: center; }
.portada .inst { font-size: 13pt; font-weight: bold; color: #1f7f7f; letter-spacing: .3pt; }
.portada .curso { font-size: 11.5pt; margin-top: 4pt; }
.portada .titulo { font-size: 25pt; color: #1f7f7f; font-weight: bold; line-height: 1.2; margin: 0 1cm; }
.portada .subtitulo { font-size: 13pt; font-style: italic; margin-top: 10pt; }
.portada .tipo { font-size: 12pt; font-weight: bold; margin-top: 26pt; letter-spacing: 1pt; text-transform: uppercase; }
.portada .rotulo { font-size: 10pt; color: #555; text-transform: uppercase; letter-spacing: 1pt; margin-top: 14pt; }
.portada .nombres { font-size: 12pt; line-height: 1.6; }
.portada .linea { width: 5cm; border-top: 1.2pt solid #1f7f7f; margin: 18pt auto; }
.sourceCode .kw, .sourceCode .cf { color: #8b2f8b; } .sourceCode .st { color: #2f6f2f; } .sourceCode .co { color: #6a737d; font-style: italic; }
.sourceCode .fu { color: #1f5f9f; } .sourceCode .dv, .sourceCode .fl { color: #b35900; } .sourceCode .bu { color: #1f7f7f; }
"""


def marcar(valor: str) -> str:
    """Escapa y resalta los valores que siguen entre corchetes (sin llenar)."""
    v = html.escape(valor)
    return f'<span class="marcador">{v}</span>' if re.fullmatch(r"\[.*\]", valor.strip()) else v


def portada(datos: dict) -> str:
    nombres = "<br>".join(marcar(n) for n in datos["integrantes"])
    return f"""<section class="portada">
<div><div class="inst">{marcar(datos['universidad'])}{' · ' + marcar(datos['programa']) if datos.get('programa') else ''}</div>
<div class="curso">{marcar(datos['curso'])}</div></div>
<div><div class="titulo">{html.escape(datos['titulo'])}</div>
<div class="subtitulo">{html.escape(datos['subtitulo'])}</div>
<div class="tipo">Informe técnico</div></div>
<div><div class="rotulo">Integrantes</div><div class="nombres">{nombres}</div>
<div class="rotulo">Docente</div><div class="nombres">{marcar(datos['docente'])}</div>
<div class="linea"></div>
<div class="nombres">{marcar(datos['ciudad'])}, {html.escape(datos['fecha'])}</div></div>
</section>
"""


def buscar_navegador() -> str:
    candidatos = ["google-chrome", "chrome", "chromium", "chromium-browser", "msedge", "microsoft-edge"]
    for c in candidatos:
        ruta = shutil.which(c)
        if ruta:
            return ruta
    if platform.system() == "Windows":
        for base in (os.environ.get("PROGRAMFILES", r"C:\Program Files"),
                     os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"),
                     os.environ.get("LOCALAPPDATA", "")):
            for rel in (r"Google\Chrome\Application\chrome.exe", r"Microsoft\Edge\Application\msedge.exe"):
                ruta = Path(base) / rel
                if ruta.exists():
                    return str(ruta)
    raise SystemExit("No se encontró Google Chrome ni Microsoft Edge para imprimir el PDF.")


def main() -> None:
    if not shutil.which("pandoc"):
        raise SystemExit("Falta pandoc: https://pandoc.org/installing.html")
    for archivo in ("secciones_1_5.md", "secciones_6_7_8.md", "portada.json"):
        if not (DIR / archivo).exists():
            raise SystemExit(f"Falta informe/{archivo} (ver el docstring de este script)")

    datos = json.loads((DIR / "portada.json").read_text(encoding="utf-8"))
    cuerpo = ((DIR / "secciones_1_5.md").read_text(encoding="utf-8").rstrip() + "\n\n"
              + (DIR / "secciones_6_7_8.md").read_text(encoding="utf-8"))
    # Las marcas [PENDIENTE: ...] de las secciones generadas se resaltan
    cuerpo = re.sub(r"\*\*\[PENDIENTE: ([^\]]+)\]\*\*", r'<span class="pendiente">**[PENDIENTE: \1]**</span>', cuerpo)
    md = portada(datos) + "\n\n" + cuerpo

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        (tmp / "estilo.css").write_text(CSS % {"encabezado": ENCABEZADO}, encoding="utf-8")
        (tmp / "informe.md").write_text(md, encoding="utf-8")
        html_final = DIR / "informe_final.html"
        subprocess.run(
            ["pandoc", str(tmp / "informe.md"), "-f", "markdown-implicit_figures+raw_html",
             "-t", "html5", "--standalone", "--embed-resources", "--resource-path", str(DIR),
             "--css", str(tmp / "estilo.css"), "--highlight-style", "pygments",
             "--metadata", "title=Informe técnico - Proyecto Nota 3", "--metadata", "lang=es",
             "--variable", "pagetitle=Informe técnico - Proyecto Nota 3",
             "-o", str(html_final)],
            check=True)
        # pandoc agrega un <header> con el título: se quita (la portada ya lo tiene)
        texto = html_final.read_text(encoding="utf-8")
        texto = re.sub(r'<header id="title-block-header">.*?</header>', "", texto, flags=re.S)
        html_final.write_text(texto, encoding="utf-8")

        navegador = buscar_navegador()
        perfil = tmp / "perfil"
        subprocess.run(
            [navegador, "--headless=new", "--disable-gpu", "--no-sandbox", f"--user-data-dir={perfil}",
             "--no-pdf-header-footer", "--virtual-time-budget=10000",
             f"--print-to-pdf={SALIDA_PDF}", html_final.resolve().as_uri()],
            check=True, capture_output=True)
    print(f"PDF generado: {SALIDA_PDF} ({SALIDA_PDF.stat().st_size / 1e6:.1f} MB)")
    pendientes = re.findall(r"\[PENDIENTE: [^\]]+\]", cuerpo) + [v for v in
                 [datos[k] for k in ("universidad", "programa", "curso", "docente", "ciudad")] + datos["integrantes"]
                 if re.fullmatch(r"\[.*\]", v.strip())]
    if pendientes:
        print("Atención, quedan valores por completar (resaltados en amarillo en el PDF):")
        for p in pendientes:
            print("  -", p)


if __name__ == "__main__":
    sys.exit(main())
