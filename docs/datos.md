# Datos

## 1. Fuentes

| Fuente | Unidad | Qué es | Dónde está |
|---|---|---|---|
| Dataset AquaVigía | 1 (batch) | Muestra **simulada** con rangos realistas inspirados en los estándares de la ANA del Perú (Brief del proyecto): 180 estaciones y 1 250 000 mediciones de 24 meses (enero de 2024 a diciembre de 2025) | `AquaVigia_Producto_U1/<integrante>/data/` (versionado) |
| USGS Water Data API | 2 (tiempo real) | Lecturas **reales** de sensores continuos, cada 5 minutos, de la colección `continuous` de `api.waterdata.usgs.gov/ogcapi/v0` | Se descarga con el productor; no se versiona |

## 2. Dataset de la Unidad 1

| Archivo | Contenido |
|---|---|
| `estaciones_agua.csv` | 180 estaciones: `estacion_id`, nombre, región, cuenca, tipo de fuente, latitud, longitud y fecha de instalación |
| `mediciones_calidad_agua.parquet` | 1 250 000 mediciones con 15 columnas: identificadores, fecha y hora, canal (`sensor_iot` o `laboratorio`), pH, temperatura, turbidez, oxígeno disuelto, conductividad, sólidos disueltos, nitratos, fosfatos, coliformes y observaciones |

Calidad, medida en el cuaderno `04`:

| Revisión | Resultado | Tratamiento |
|---|---|---|
| Columnas obligatorias | Presentes | Si falta una, el cuaderno se detiene |
| Nulos en las variables de sensor | 0 % | |
| Nulos en nitratos, fosfatos y coliformes | 88 %: solo el laboratorio los mide | Se dejan nulos. Inventar un valor sería peor |
| Duplicados por estación, fecha y hora | 4 175 grupos | Se conserva uno por grupo con `Window` y `row_number`: quedan **1 245 817** filas |
| Filas sin identificador crítico | 0 | `na.drop` sobre los identificadores |

Capas:

- **Bronze:** los dos archivos tal como llegan.
- **Silver:** mediciones validadas y sin duplicados.
- **Gold:** mediciones integradas con sus estaciones, en `artifacts/mediciones_particionado/`, Parquet con `partitionBy("region")`: Arequipa, Cusco, Junín, Lambayeque, Lima, Loreto, Piura y Puno. La región tiene baja cardinalidad y es el filtro más usado, así que Spark solo lee la carpeta que necesita.

## 3. Historial del USGS (Unidad 2)

| Dato | Valor |
|---|---|
| Archivo | `AquaVigia_Producto_U2/data/historial.jsonl` (una lectura por línea) |
| Periodo de nuestra descarga | 1 de agosto a 30 de octubre de 2026 (90 días) |
| Lecturas | 1 309 777 |
| Estaciones | 13 |
| Variables | temperatura (336 100), pH (335 093), turbidez (328 681) y oxígeno disuelto (309 903) |
| Tamaño | unos 430 MB, por eso no se versiona |

Con él se entrenan los modelos del 12b, y el productor lo reenvía (`reproducir`) para las pruebas y la demostración. Los contratos de los eventos están en [Arquitectura](arquitectura.md#contratos-de-los-eventos).

## 4. Tópicos de Kafka

| Tópico | Particiones | Key | Contenido |
|---|---|---|---|
| `agua-usgs-eventos-v2` | 3 | `estacion_id` | Lecturas de sensor, contrato versión 2 |
| `orden-eventos` | 1 | estación y variable | Órdenes de inspección (Patricia) |
| `alerta-eventos` | 1 | estación y variable | Alertas de calidad (Jhenderson) |

La creación automática de tópicos está desactivada. Los crea `scripts/crear_topicos.ps1`.

## 5. Artefactos generados

Ninguno se versiona: se regeneran con los cuadernos ([Instalación](instalacion.md#5-generar-los-artefactos-una-sola-vez)).

| Ruta, dentro de `pyspark/sesiones/` | Lo genera | Contenido |
|---|---|---|
| `AquaVigia_Producto_U1/<integrante>/artifacts/mediciones_particionado/` | `04` | Parquet gold por región |
| `AquaVigia_Producto_U1/patricia/artifacts/modelo_alerta_siguiente_mes/` | `04` de Patricia | Modelo MLlib |
| `AquaVigia_Producto_U1/jhenderson/artifacts/modelo_ph_siguiente_mes/` | `04` de Jhenderson | Modelo MLlib |
| `AquaVigia_Producto_U2/<integrante>/artifacts/modelos_<integrante>/` | `12b` | Un modelo por variable y `meta.json` (RMSE, línea base, umbrales p05 y p95 de cada estación) |
| `AquaVigia_Producto_U3/<integrante>/artifacts/manifiestos/` | `13` | Un manifiesto JSON por corrida |

## 6. Manifiesto de ejecución

Cada corrida del cuaderno 13 deja un JSON con:

| Bloque | Qué registra |
|---|---|
| `runId`, `inicioUtc`, `finUtc`, `duracionTotalS` | Identidad y duración de la corrida |
| `versiones`, `parametros` | Versiones de Spark y Python, y los parámetros (duración, disparador, umbral, watermark) |
| `compuertasPrevias`, `kafka` | Resultado de cada compuerta previa y mensajes en los tópicos al empezar |
| `batch` | Filas, regiones, percentiles y compuertas de calidad del Parquet |
| `streaming` | Micro-lotes, pronósticos, eventos emitidos y confirmados en Kafka, riesgo máximo y error en vivo |
| `kpiTiempo` | Latencia y duración del micro-lote en p50, p95 y máximo |
| `observabilidad` | Métricas publicadas y si Prometheus las devolvió |
| `advertencias` | Lo que no detiene la corrida pero hay que revisar |

Copias de los manifiestos reales en [`u3/evidencias/manifiestos/`](https://github.com/Patricia-Apaza/Big-Data-Contaminacion-del-Agua-/tree/main/docs/u3/evidencias/manifiestos).
