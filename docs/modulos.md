# Módulos y funcionalidades

Cada módulo responde a una parte de la pregunta central: **¿cómo anticipar el deterioro de la calidad del agua combinando el histórico con las lecturas en tiempo real?**

| Módulo | Dónde está | Qué hace | Qué parte del problema resuelve |
|---|---|---|---|
| Pipeline batch | `AquaVigia_Producto_U1/<integrante>/04_producto_u1_<integrante>.ipynb` | Lee 1 250 000 mediciones, valida el esquema, trata nulos y duplicados (quedan 1 245 817), integra las estaciones y escribe Parquet particionado por región | Deja el histórico limpio y consultable por región |
| Modelo batch (U1) | mismo cuaderno | Patricia: clasificación de la alerta del mes siguiente (LogisticRegression). Jhenderson: regresión del pH promedio del mes siguiente (LinearRegression) | Proyección mensual por estación |
| Productor del USGS | `AquaVigia_Producto_U2/patricia/usgs_productor.py` | `historial`: baja N días. `publicar`: lecturas nuevas en vivo. `reproducir`: reenvía un tramo del historial. Agrega a cada lectura su contexto (rezagos y variables simultáneas) | Trae las lecturas reales como lo haría un dispositivo de campo |
| Puente MQTT a Kafka | `uso-agua-usgs/app/bridge_mqtt_kafka_usgs.py` | Se suscribe a `aquavigia/equipo02/usgs/#` y reenvía a `agua-usgs-eventos-v2` con key = estación y cabecera `canal=mqtt`. Reintenta si Kafka no está | Separa el protocolo de campo (MQTT) del bus de eventos (Kafka) |
| Ingesta y ventanas | `12a_streaming_usgs_<integrante>_v2.ipynb` | Ingesta verificada, ventana de 15 min, watermark medido de 85 min, checkpoint y prueba de dato tardío | Procesa el flujo por tiempo del evento aunque llegue tarde |
| Modelo de series de tiempo (U2) | `12b_modelo_usgs_<integrante>.ipynb` | Pronóstico a 1 hora por variable, validado contra repetir el último valor y contra la media. Guarda los modelos, el RMSE y los umbrales p05 y p95 por estación | Anticipa el valor de la próxima hora |
| Negocio y observabilidad | `12c_negocio_usgs_<integrante>_v3.ipynb` | Deduplica, pronostica sin estado, calcula el riesgo, publica el evento de negocio, mide el error en vivo y publica métricas | Convierte el pronóstico en una decisión: orden de inspección o alerta |
| Semántica de entrega | `12d_ingesta_semantica_<integrante>.ipynb` | Contrato versionable, partición por key, entrega al menos una vez y deduplicación, ventana fija frente a deslizante | Evita decisiones duplicadas o perdidas |
| Evaluación del riesgo | `12e_evaluacion_riesgo_<integrante>.ipynb` | Curva ROC del riesgo y curva de aprendizaje del modelo | Mide si el riesgo separa bien los casos reales |
| Ejecución integrada | `AquaVigia_Producto_U3/<integrante>/13_integracion_<integrante>.ipynb` | Batch, streaming, modelo, eventos y métricas en una sola corrida con compuertas y manifiesto | Demuestra que todo funciona junto y deja registro auditable |
| Tablero | `obs/grafana/provisioning/dashboards/json/aquavigia_u3.json` | Grafana lo carga solo al arrancar. Filas: ejecución integrada de cada integrante, infraestructura compartida y pipeline de cada integrante | Muestra en una pantalla el estado, el pronóstico y el riesgo |
| Arranque y pruebas | `scripts/` | `levantar.ps1`, `crear_topicos.ps1`, `probar_salud.ps1`, `pruebas_hardening.ps1` | Hace el sistema reproducible por otra persona |

## Cómo se calcula el riesgo

1. El modelo pronostica el valor de la próxima hora.
2. Su error se modela como normal, con desviación igual al RMSE de validación del 12b.
3. El riesgo es la probabilidad de que el valor real quede fuera del rango de la estación (percentiles 5 y 95 del entrenamiento), en la dirección que preocupa:

| Variable | Dirección que preocupa |
|---|---|
| Temperatura | alta |
| Oxígeno disuelto | baja |
| pH | ambas |
| Turbidez | alta |

4. Si el riesgo es 0,5 o más, se emite el evento de negocio, salvo que ya se haya emitido uno para esa estación y variable en los últimos 60 minutos.

Los umbrales son estadísticos, de cada estación. No son una norma de calidad del agua.
