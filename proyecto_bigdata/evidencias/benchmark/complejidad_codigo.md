**Tabla 14.** *Complejidad del código por etapa del pipeline: líneas de código efectivas / funciones / complejidad ciclomática máxima de una función*

| Etapa | pandas | PySpark |
|---|---|---|
| Configuración del motor | - | 56 / 4 / 6 |
| Carga | 78 / 3 / 8 | 41 / 3 / 3 |
| Limpieza | 45 / 3 / 8 | 53 / 4 / 2 |
| Joins | 59 / 4 / 4 | 14 / 4 / 1 |
| Agregaciones (4 consultas) | 108 / 6 / 4 | 85 / 7 / 1 |
| Escritura Parquet | 23 / 1 / 2 | 17 / 2 / 1 |
| Mitigación por chunks | 139 / 2 / 18 | - |
| Orquestación (CLI) | 132 / 3 / 9 | 160 / 3 / 9 |
| **Total** | **584 / 22 / 18** | **426 / 27 / 9** |
| Complejidad ciclomática media por función | 4,23 | 1,89 |
| Anidamiento máximo de bloques | 4 | 2 |

Líneas efectivas: sin líneas en blanco, comentarios ni docstrings. Complejidad ciclomática de McCabe: 1 + número de decisiones (if, bucles, except, and/or). Se excluyen los scripts de prueba, validación y benchmark. Medido con `benchmark/complejidad_codigo.py`.
