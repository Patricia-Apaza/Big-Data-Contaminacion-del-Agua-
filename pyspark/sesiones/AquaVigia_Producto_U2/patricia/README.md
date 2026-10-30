# AquaVigia U2: pipeline en tiempo real con datos reales del USGS

Equipo 02, Big Data (UPeU). Dimensión de Patricia Apaza: temperatura y oxígeno disuelto. Dimensión de Jhenderson Machaca: pH y turbidez.

## Qué hace

Pipeline Lambda sobre sensores reales de calidad de agua del USGS (EE. UU.), no simulados.

- **Capa batch:** historial de 90 días (unas 1,3 millones de lecturas) para entrenar el modelo.
- **Capa de velocidad:** lecturas nuevas del USGS a Kafka (`agua-usgs-eventos`), procesadas con Spark Structured Streaming (ventana de 15 min, watermark y checkpoint).
- **Modelo:** pronóstico de series de tiempo a 1 hora, validado contra "repetir el último valor" y contra la media.
- **Evento de negocio:** orden de inspección (`orden-eventos`) en el caso de Patricia, alerta de calidad (`alerta-eventos`) en el de Jhenderson.
- **Observabilidad:** métricas al Pushgateway, Prometheus y un tablero común en Grafana.

## Archivos

| Archivo | Para qué sirve |
|---|---|
| `usgs_productor.py` | Descarga el historial y publica lecturas nuevas del USGS a Kafka |
| `12a_streaming_usgs_<nombre>_v2.ipynb` | Ingesta verificada, ventana, watermark, checkpoint, dato tardío y salida Parquet |
| `12b_modelo_usgs_<nombre>.ipynb` | Entrenamiento y validación del modelo de series de tiempo |
| `12c_negocio_usgs_<nombre>_v2.ipynb` | Modelo en streaming, evento de negocio y métricas |
| `grafana_aquavigia_u2_usgs.json` | Tablero de Grafana (importar con Prometheus como fuente) |
| `COSTOS_U2_Patricia.md` | Estimación de costos y criterios de escalado |

## Cómo reproducirlo

Requisitos: Docker con Kafka, Prometheus, Grafana, Pushgateway y el contenedor de PySpark del curso; Python con `kafka-python`; una clave gratuita del USGS (https://api.waterdata.usgs.gov/signup).

1. Crear el tópico: `kafka-topics.sh --create --topic agua-usgs-eventos --partitions 1 --replication-factor 1`.
2. Poner la clave **solo como variable de entorno** (nunca en el repositorio): `set USGS_API_KEY=...`.
3. Historial: `py usgs_productor.py historial --estaciones <lista> --dias 90 --variables temperatura_c,oxigeno_mgl,ph,turbidez_fnu`. El archivo `historial.jsonl` va en `data/` y no se sube (supera el límite de GitHub).
4. Tiempo real: `py usgs_productor.py publicar --estaciones <lista> --cada 120 --salida kafka`.
5. Ejecutar en orden `12a`, `12b` y `12c` con el archivo de cada integrante.

## Hallazgos y límites (declarados)

- El USGS mide cada 5 minutos pero publica por tandas con 40 a 54 minutos de retraso. El watermark se eligió de esa medición (85 minutos con una ventana limpia de unos 34 minutos y 144 eventos; muestra pequeña).
- Las pruebas de reinicio y de dato tardío reenvían lecturas reales duplicadas a Kafka para generar trabajo. Se declaran como pruebas.
- El umbral de alerta de operación es 0.5. Para la demostración se usó 0.01, y con él se publicó una sola orden. Con el umbral de operación no hubo órdenes porque no había riesgo.
- Las órdenes se publican solo en Kafka, sin conectar el servicio de órdenes de S6.
- Los modelos vienen de un solo tramo de prueba de unas 3 semanas (desde el 12 de octubre de 2026), no de validación repetida.
