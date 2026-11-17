# Versión pandas (a cargo de otro integrante)

Use los módulos comunes para garantizar una comparación justa:

```python
import sys; from pathlib import Path; sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.muestras import rutas_muestra, DIR_DIMENSIONES
from comun.esquemas import DTYPES_PANDAS_TRANSACCIONES   # mitigación: tipos optimizados
```

Mitigaciones que el enunciado exige probar y documentar: `chunksize`, `dtype`, `usecols`.
Ver `datos/DESCRIPCION_DATOS.md`, §7, para las operaciones que no se resuelven por chunks.
