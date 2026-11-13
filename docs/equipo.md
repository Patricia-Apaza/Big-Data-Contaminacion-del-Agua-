# Equipo y aporte individual

## Integrantes

| Integrante | Usuario de GitHub | Énfasis definido en el Brief | Variables |
|---|---|---|---|
| Celia Patricia Apaza Hilasaca | `Patricia-Apaza` | Streaming y Kafka, observabilidad: ingesta en tiempo real, tableros de Grafana y alertas | temperatura y oxígeno disuelto |
| Jhenderson Aaron Machaca Mamani | `JhendersonAaron` | Batch y Spark: diseño del dataset, pipeline de transformación histórica y modelos predictivos de la U1 | pH y turbidez |

El Brief aclara que el énfasis es de coordinación: los dos integrantes trabajan las dos capas, cada uno con sus propias dimensiones.

## Qué hizo cada uno

| Unidad | Patricia Apaza | Jhenderson Machaca |
|---|---|---|
| 1 (batch) | Cuaderno `04_producto_u1_patricia`: porcentaje de mediciones fuera de rango por estación y mes, Parquet gold y clasificador de la alerta del mes siguiente | Cuaderno `04_producto_u1_jhenderson`: pH y turbidez promedio por estación y mes, Parquet gold y regresión del pH del mes siguiente |
| 2 (tiempo real) | Productor del USGS (`usgs_productor.py`); puente MQTT a Kafka; cuadernos `12a` a `12e` de temperatura y oxígeno; orden de inspección en `orden-eventos`; costos de su dimensión | Cuadernos `12a` a `12e` de pH y turbidez; alerta de calidad en `alerta-eventos`; costos de su dimensión |
| 3 (integración) | Infraestructura en el repositorio (`kafka/`, `obs/`, `uso-agua-usgs/`, `scripts/`), hardening y su bitácora, tablero de Grafana U3, cuaderno `13` de su dimensión, documentación MkDocs | Cuaderno `13` de su dimensión, corrección de su Parquet gold (caso de error documentado), corridas antes y después del hardening |
| Compartido | Arquitectura y Brief del proyecto; tablero común de Grafana; sustentaciones | Ídem |

## Evidencia en el repositorio

El historial empieza el 10 de septiembre de 2026 y tiene commits de los dos usuarios a lo largo de las tres unidades. Los de cada integrante se ven con:

```
git log --author="Patricia-Apaza" --format="%h %ad %s" --date=short
git log --author="JhendersonAaron" --format="%h %ad %s" --date=short
```

| Fecha | Commit | Autor | Qué agrega |
|---|---|---|---|
| 10-sep | `a4ccb4e`, `feb4e06` | Jhenderson, Patricia | Producto de la Unidad 1 de cada uno |
| 30-oct | `6eee92f` | Patricia | Productor del USGS |
| 30-oct | `f3f047c` a `b7657ad` | Patricia | 12a, 12b, 12c, reproducción y costos |
| 30-oct | `6aa8ada` a `27ee3d2` | Jhenderson | 12a, 12b, 12c, reproducción y costos |
| 30-oct | `285b0dc`, `606021d` | Patricia, Jhenderson | Sitio MkDocs y páginas de cada dimensión |
| 06-nov | `a76756a`, `ab9b60e` | Patricia, Jhenderson | Versión 3 de la U2: MQTT, inferencia sin estado, error en vivo, ROC |
| 13-nov | `f3d716d` | Patricia | Infraestructura en el repositorio y hardening |
| 13-nov | `ce4e8b6`, `1603642` | Patricia, Jhenderson | Ejecución integrada de cada uno |
| 13-nov | `0f485e9`, `1cec60d` | Patricia, Jhenderson | Hardening aplicado, no regresión y parada ordenada |
| 13-nov | `7123878`, `70d3959` | Patricia, Jhenderson | Documentación MkDocs de la U3 y rutas de la U2 dentro del repositorio |

En la Unidad 3 el equipo trabajó en una laptop compartida. Cada commit de una dimensión se registró con el usuario de git de su integrante.
