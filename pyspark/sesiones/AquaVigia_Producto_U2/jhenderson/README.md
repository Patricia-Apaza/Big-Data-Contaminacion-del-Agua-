# AquaVigía U2 (Jhenderson Machaca): pipeline en tiempo real con datos reales del USGS

Equipo 02, Big Data (UPeU). Mi dimensión: **pH y turbidez**. Mi evento de negocio: **alerta de calidad** (`alerta-eventos`). La dimensión de Patricia Apaza (temperatura y oxígeno) está en `../patricia/`; las dos comparten Kafka, el tópico de sensores `agua-usgs-eventos` y el tablero de Grafana.

## Archivos

| Archivo | Para qué sirve |
|---|---|
| `u2-producto.md` | Documento de la U2 con la evidencia medida |
| `12a_streaming_usgs_jhenderson_v2.ipynb` | Ingesta verificada, ventana, watermark, checkpoint, dato tardío y salida Parquet |
| `12b_modelo_usgs_jhenderson.ipynb` | Entrenamiento y validación del modelo de series de tiempo |
| `12c_negocio_usgs_jhenderson_v2.ipynb` | Modelo en streaming, alerta de calidad y métricas para Grafana |
| `grafana_aquavigia_u2_usgs.json` | Tablero de Grafana (importar con Prometheus como fuente) |
| `COSTOS_U2_Jhenderson.md` | Estimación de costos y criterios de escalado |

El productor `usgs_productor.py` está en `../patricia/` (es compartido).

## Como reproducirlo

Los cuadernos se ejecutan dentro del contenedor de PySpark del curso, con la carpeta `sesiones/s12-u2-usgs` montada como `/opt/s12-u2-usgs` (ahi viven `data/historial.jsonl` y `artifacts/`, que no se suben al repositorio). Los pasos completos estan en la sección 8 de `u2-producto.md`. Resumen:

1. Crear el tópico: `kafka-topics.sh --create --topic alerta-eventos --partitions 1 --replication-factor 1`.
2. Poner la clave del USGS **solo como variable de entorno**: `set USGS_API_KEY=...`. Nunca se sube a git.
3. Correr en orden `12a`, `12b` y `12c`, celda por celda y con un solo kernel abierto.

## Resultados y limites (declarados)

- Ingesta: 9 867 mensajes, 100 % válidos; 3 600 de mis variables. Watermark de 85 minutos medido con una ventana limpia de 138 eventos (muestra pequeña). Reinicio con checkpoint: batchId continua en 5. Dato tardío: se descarta con watermark y reaparece sin el.
- Modelo: pH mejora 9,4 % y turbidez 4,2 % en RMSE frente a repetir el último valor, pero el MAE es peor que esa referencia en las dos variables.
- Alertas: 7 alertas de pH con umbral 0,5, ninguna de turbidez. Los umbrales de advertencia son estadísticos (percentiles 5 y 95 por estación), no una norma.
- El 12c se ejecutó en modo reproducción, sobre lecturas ya guardadas.
- Las alertas se publican solo en Kafka. El tópico de sensores es compartido y contiene reenvíos de prueba.

## Versión 3 (evaluación S12)

| Archivo | Para qué sirve |
|---|---|
| `../patricia/usgs_productor.py` | Contrato v2 (`version_esquema`, `contexto`) y salida `--salida mqtt`; `reproducir --desde/--hasta` para un episodio |
| `../puente_mqtt/` | Mosquitto y el puente MQTT a Kafka (`docker compose up -d --build`) |
| `12d_ingesta_semantica_jhenderson.ipynb` | S6 a S8: particiones, contrato versionado, key por estación, MQTT, rango físico, deduplicación, ventanas |
| `12c_negocio_usgs_jhenderson_v3.ipynb` | Inferencia sin estado, alerta de calidad, error en vivo, métricas batch, lag y throughput |
| `grafana_aquavigia_u2_usgs_v3.json` | Tablero v3 con salud del pipeline, KPIs, predicción y batch frente a streaming |

Tópico nuevo: `agua-usgs-eventos-v2` (3 particiones). Episodio de la demo: `py usgs_productor.py reproducir --archivo historial.jsonl --desde 2026-10-16T22:00 --hasta 2026-10-17T04:00 --velocidad 30 --salida mqtt`.
