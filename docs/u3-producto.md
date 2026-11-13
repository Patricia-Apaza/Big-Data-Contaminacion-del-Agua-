# Producto de la Unidad 3 (Producto Final)

Instrumento `u3-producto.md` del curso, adaptado a AquaVigía. Según el sílabo, el Producto Final es el producto de la Unidad 3: el mismo sistema de las Unidades 1 y 2, integrado, endurecido, documentado y defendido.

**Producto:** sistema Big Data distribuido de punta a punta, para procesamiento batch y streaming, analítica y ML, observabilidad y visualización BI, orientado a la toma de decisiones sobre la calidad del agua.

## 1. Componentes mínimos y dónde está cada uno

| Componente exigido | En AquaVigía | Evidencia |
|---|---|---|
| Problema de datos delimitado y arquitectura justificada (Lambda o Kappa) | Anticipar el deterioro de la calidad del agua; arquitectura Lambda | [Inicio](index.md), [Arquitectura](arquitectura.md#decisiones-tecnicas-y-lo-que-cuestan) |
| Pipeline batch con Spark: extracción, transformaciones, agregaciones y validación de calidad | Cuaderno `04` de cada integrante | [Datos](datos.md#2-dataset-de-la-unidad-1), [Evidencias](evidencias.md#3-batch-unidad-1) |
| Salida analítica en Parquet particionado | `mediciones_particionado`, `partitionBy("region")`, 1 245 817 filas | [Datos](datos.md#2-dataset-de-la-unidad-1) |
| Ingesta de eventos empresariales e IoT con Kafka | Sensores del USGS por MQTT a `agua-usgs-eventos-v2`; órdenes y alertas en `orden-eventos` y `alerta-eventos` | [Arquitectura](arquitectura.md#contratos-de-los-eventos) |
| Streaming con Spark Structured Streaming (ventanas, watermark) | Ventana de 15 min, watermark medido de 85 min, checkpoint, deduplicación | Cuadernos `12a`, `12c` y `13`; [Evidencias](evidencias.md#4-tiempo-real-unidad-2) |
| Observabilidad con métricas, paneles y costos | Prometheus, Pushgateway, Kafka Exporter y tablero de Grafana; costos por integrante | [Uso](uso.md), costos de [Patricia](patricia/costos.md) y [Jhenderson](jhenderson/costos.md) |
| Modelo ML, series de tiempo o salida BI | Modelos MLlib de la U1; pronóstico a 1 hora con riesgo y evento de negocio en la U2 | [Módulos](modulos.md), [Evidencias](evidencias.md) |
| DataOps: comprobación de salud y espera por dependencias, configuración fuera del código, ejecución integrada con compuertas y manifiesto | `healthcheck` en 8 servicios, `depends_on: service_healthy`, `up -d --wait`, credenciales por variables, cuaderno `13` con compuertas y manifiesto | [Hardening](u3/hardening.md), [Ejecución integrada](u3/integracion.md) |
| Revisión técnica final: checklist, no regresión, riesgos aceptados | Lista de comprobación aplicada, prueba antes y después, 7 riesgos aceptados con justificación | [Hardening](u3/hardening.md) |
| Evidencias reproducibles de ejecución | Manifiestos JSON de cada corrida y salidas de las pruebas | [Evidencias](evidencias.md) |
| Documentación técnica, operativa y demo de punta a punta | Este sitio; la demo es la ejecución integrada, con guion y plan alternativo | [Instalación](instalacion.md), [Uso](uso.md#1-ejecucion-integrada-recomendada), [Guion de demostración](u3/demo.md) |

## 2. Entregables de S15

| Entregable | Evidencia mínima | En AquaVigía |
|---|---|---|
| Producto final | `u3-producto.md` adaptado | Esta página |
| Integración de punta a punta | Batch, streaming y BI/ML juntos, con al menos un caso de error | Cuaderno `13`; caso de error: Parquet incompleto detenido por la compuerta de completitud ([bitácora](u3/integracion.md#5-bitacora-de-integracion)) |
| DataOps y hardening | Empaquetado o despliegue reproducible; checklist completado | `scripts/levantar.ps1`; [lista de comprobación](u3/hardening.md#2-lista-de-comprobacion-de-configuracion) |
| Repositorio y documentación | Topics, MkDocs publicado, informe | [Repositorio](repositorio.md); informe del proyecto |
| Sustentación con aporte individual | Pitch, exposición, demo y defensa por integrante | [Equipo y aporte](equipo.md) |

## 3. Rúbrica de evaluación (Tabla 1 del instrumento)

La calificación la asigna el docente. La última columna indica dónde está la evidencia de cada criterio.

| Criterio | Peso | Dónde está la evidencia |
|---|---|---|
| 1. Integración end-to-end de batch, streaming y BI/ML | 20 % | Cuaderno `13` de cada integrante y sus manifiestos; fila U3 de Grafana |
| 2. Prácticas DataOps/DevOps aplicadas al empaquetado o despliegue | 15 % | `scripts/`, `healthcheck` y `depends_on`, `.env.example`, compuertas y manifiesto |
| 3. Documentación operativa completa | 15 % | Este sitio, en especial [Instalación](instalacion.md) y [Uso](uso.md) |
| 4. Hardening y revisión técnica final | 10 % | [Hardening](u3/hardening.md): checklist, bitácora, riesgos aceptados y no regresión |
| 5. Demo reproducible | 15 % | [Guion de demostración](u3/demo.md) y [Uso, sección 1](uso.md#1-ejecucion-integrada-recomendada) |
| 6. Sustentación técnica con evidencias, métricas, resultados y valor para la toma de decisiones | 25 % | Presentación, demo y defensa individual |

## 4. Valor para la toma de decisiones

| Decisión | Quién la toma | Qué le da el sistema |
|---|---|---|
| Enviar una inspección a una estación antes de que la temperatura suba o el oxígeno baje | Responsable de operación | Orden de inspección en `orden-eventos` con el valor actual, el pronóstico a una hora y el riesgo |
| Avisar de un cambio de pH o turbidez | Responsable de calidad | Alerta de calidad en `alerta-eventos` con los mismos datos |
| Confiar o no en el pronóstico | Analista | Error del modelo medido en vivo frente a repetir el último valor, en Grafana y en cada manifiesto |
| Saber si el sistema está funcionando | Quien opera el sistema | Antigüedad de la última ejecución, latencias y compuertas en Grafana |
