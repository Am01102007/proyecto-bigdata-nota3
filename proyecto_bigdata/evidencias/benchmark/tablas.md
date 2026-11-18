**Tabla 11.** *Tiempo de ejecución por variante y muestra (una ejecución por combinación, mediciones de la sección 5) y aceleración de Spark*

| Muestra | Filas | CSV (GB) | pandas directo | pandas mitigado | PySpark | Spark vs. directo | Spark vs. mitigado |
|---|---:|---:|---|---|---|---:|---:|
| 1 % | 1.235.843 | 0,12 | 12,8 s | 7,5 s | 18,2 s | 0,70 × | 0,41 × |
| 10 % | 12.358.430 | 1,15 | 112,4 s | 58,3 s | 34,5 s | 3,26 × | 1,69 × |
| 50 % | 61.792.150 | 5,77 | MemoryError tras 2,3 min | 6,4 min | 92,1 s | solo Spark termina | 4,19 × |
| 100 % | 123.584.300 | 11,53 | MemoryError tras 3,5 min | MemoryError tras más de 1,5 h | 3,0 min | solo Spark termina | solo Spark termina |

La aceleración es el tiempo de pandas dividido entre el de Spark (mayor que 1: Spark es más rápido).

**Tabla 12.** *Pico de memoria RAM en GB por variante y muestra (mediciones de la sección 5)*

| Muestra | pandas directo | pandas mitigado | PySpark |
|---|---|---|---|
| 1 % | 1,20 | 0,72 | 1,45 |
| 10 % | 4,15 | 1,85 | 2,10 |
| 50 % | No registrado | 4,95 | 3,80 |
| 100 % | No registrado | No registrado | 4,60 |

RAM física del equipo: 7,64 GB. «No registrado»: la ejecución falló antes de que se registrara el pico de memoria.

