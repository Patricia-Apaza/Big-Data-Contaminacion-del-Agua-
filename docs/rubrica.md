# Trazabilidad con la rúbrica de la Unidad 2

| Criterio | Peso | Patricia Apaza | Jhenderson Machaca |
|---|---|---|---|
| 1. Ingesta de eventos empresariales con Kafka | 12 % | 1 orden de inspección en `orden-eventos`, contrato documentado | 7 alertas de calidad en `alerta-eventos`, contrato documentado |
| 2. Ingesta de eventos IoT y sensores | 10 % | 6 072 mensajes, 100 % válidos, 2 323 únicos | 9 867 mensajes, 100 % válidos, 3 553 únicos |
| 3. Procesamiento streaming con Spark | 14 % | ventana 15 min, watermark 85 min medido, checkpoint (batchId continúa en 4), dato tardío probado | ventana 15 min, watermark 85 min medido, checkpoint (batchId continúa en 5), dato tardío probado |
| 4. Observabilidad | 12 % | 8 métricas en Prometheus, fila propia en Grafana | 8 métricas en Prometheus, fila propia en Grafana |
| 5. Costos y escalado | 8 % | volumen medido, dos opciones con precios verificados, tres criterios de escalado | lo mismo, con su volumen |
| 6. Salidas BI y ML | 12 % | KPIs, pronóstico y riesgo en el mismo tablero | KPIs, pronóstico y riesgo en el mismo tablero |
| 7. Series de tiempo validadas | 12 % | +45,1 % y +32,1 % de RMSE sobre la referencia | +9,4 % y +4,2 % de RMSE; el MAE es peor que la referencia (declarado) |
| 8. Sustentación | 20 % | presentación y demostración en vivo | presentación y demostración en vivo |

## Límites declarados

- El watermark se midió con una muestra pequeña (144 y 138 eventos).
- El modelo se validó con una sola partición cronológica de unas dos semanas y media.
- Los umbrales son estadísticos (percentiles 5 y 95 por estación), no una norma de calidad del agua.
- Patricia usó un umbral de demostración de 0,01 para mostrar una orden; Jhenderson usó el de operación, 0,5.
- Las órdenes y alertas se publican solo en Kafka; todo corre en una laptop compartida.
