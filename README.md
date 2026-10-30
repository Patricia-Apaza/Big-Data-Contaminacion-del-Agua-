# AquaVigía: monitoreo de la calidad del agua con Big Data

Proyecto del curso Big Data (UPeU, IX ciclo). Equipo 02: Patricia Apaza y Jhenderson Machaca. Docente: Abel Sullón.

## Qué es

AquaVigía es un pipeline de datos para vigilar la calidad del agua. Cada integrante trabaja una dimensión distinta:

| Integrante | Variables |
|---|---|
| Patricia Apaza | temperatura (°C) y oxígeno disuelto (mg/L) |
| Jhenderson Machaca | pH y turbidez (FNU) |

## Unidades

### Unidad 1: producto de datos por lotes
Carpeta: `pyspark/sesiones/AquaVigia_Producto_U1/`, con una subcarpeta por integrante.

### Unidad 2: pipeline en tiempo real con datos reales
Carpeta: `pyspark/sesiones/AquaVigia_Producto_U2/`, con una subcarpeta por integrante.

La Unidad 2 usa lecturas reales de sensores del USGS (EE. UU.) en lugar de datos simulados:

- **Capa batch:** historial de 90 días (unas 1,3 millones de lecturas) para entrenar el modelo.
- **Capa de velocidad:** lecturas nuevas del USGS a Kafka, procesadas con Spark Structured Streaming (ventana, watermark y checkpoint).
- **Modelo:** pronóstico de series de tiempo a 1 hora, validado contra una línea base.
- **Evento de negocio:** orden de inspección (Patricia) y alerta de calidad (Jhenderson) publicadas en Kafka.
- **Observabilidad:** métricas con Pushgateway y Prometheus, tablero en Grafana y estimación de costos.

Los pasos para reproducirla están en `pyspark/sesiones/AquaVigia_Producto_U2/patricia/README.md`.

## Tecnologías

Python, PySpark, Apache Kafka, Prometheus, Grafana, Docker, Jupyter.

## Notas

- Los datos y los modelos entrenados no se suben al repositorio por su tamaño (carpetas `data/` y `artifacts/` ignoradas).
- La clave de la API del USGS se usa solo como variable de entorno y nunca se versiona.
