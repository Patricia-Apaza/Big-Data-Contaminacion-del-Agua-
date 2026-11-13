# Limitaciones y mejoras

## Limitaciones del producto

| Limitación | Por qué no se resolvió dentro del curso |
|---|---|
| El dataset de la Unidad 1 es **simulado**. Por eso los modelos de la U1 no encuentran señal: el clasificador de Patricia tiene AUC 0,509, igual al azar, y la regresión de Jhenderson, R² 0,0008. | El Brief lo declaró y dejó pendiente evaluar su reemplazo por datos reales del portal de datos abiertos del Estado peruano. En la Unidad 2 se pasó a datos reales del USGS. |
| Los datos reales son de **estaciones de Estados Unidos**, no del Perú. | Era la fuente pública con sensores continuos cada 5 minutos y una API documentada. El pipeline no depende del país: cambiar de fuente es cambiar el productor. |
| Cada modelo de la U2 se validó con **una sola partición cronológica** de unas 2,5 semanas. | El historial disponible es de 90 días. La validación repetida en varios cortes queda como mejora. |
| Los umbrales de riesgo son **estadísticos** (percentiles 5 y 95 de cada estación), no una norma de calidad del agua. | Una norma depende del uso del agua (consumo, riego, ecosistema), y eso está fuera del alcance. |
| En turbidez el modelo apenas mejora a repetir el último valor (4,2 % de RMSE en validación), y en pH el MAE en vivo fue un poco peor que el de la persistencia. | Son variables con cambios bruscos o muy estables. Se publica igual, con el resultado declarado. |
| El **watermark** de 85 minutos se midió con una muestra pequeña de retrasos (144 y 138 eventos). | Se midió en una ventana limpia de unos 34 minutos de la API real. |
| Todo corre en **una sola laptop**, con un broker de Kafka y Spark en modo local. | Es el laboratorio del curso. El escalado está estimado en las páginas de costos de cada integrante. |
| La ejecución integrada **reproduce un episodio** del historial; no se demostró una alerta con datos en vivo. | Reproducir un tramo conocido hace la demostración repetible. En vivo, que haya una alerta depende de lo que publique el USGS ese día. El productor sí funciona en vivo (`publicar`). |
| Al reproducir, el streaming solo ve lo que llega después de arrancar (`latest`), así que dos corridas no procesan exactamente las mismas lecturas. | Es el comportamiento de un stream real. Por eso el manifiesto registra cada corrida y la comparación es por compuertas, no valor por valor. |
| Los eventos de negocio solo se publican en Kafka: no hay un sistema que atienda las órdenes y alertas. | El consumidor final (un sistema de mantenimiento) está fuera del alcance. |

## Riesgos aceptados

Son decisiones conscientes de configuración, con su justificación y lo que cambiaría en producción. Están en [Hardening, sección 3](u3/hardening.md#3-riesgos-aceptados):

- Kafka sin volumen persistente.
- Kafka y Mosquitto sin autenticación ni cifrado.
- Grafana con `admin`/`admin` por defecto.
- Jupyter con token fijo y 9 puertos publicados.
- El Pushgateway conserva el último valor.
- La imagen base del puente sin el parche fijado.
- La clave del USGS en una variable de la terminal.

## Mejoras priorizadas

| Prioridad | Mejora | Qué resuelve |
|---|---|---|
| 1 | Volumen persistente para Kafka y más de un broker con replicación | Recrear Kafka ya no pierde los tópicos |
| 2 | Correr el streaming con `spark-submit` como servicio que exponga `/metrics`, en lugar de un cuaderno | Quita la dependencia del Pushgateway y del kernel de Jupyter |
| 3 | Validación cronológica repetida (varios cortes) y reentrenamiento periódico con el historial nuevo | Errores del modelo más confiables y modelos al día |
| 4 | Un consumidor de `orden-eventos` y `alerta-eventos` que cambie el estado (atendida, cancelada por falsa alarma) | Cierra el ciclo de la decisión, previsto en el Brief |
| 5 | Autenticación en Kafka, Mosquitto, Jupyter y Grafana, y secretos en un gestor | Lleva el sistema fuera de una red local |
| 6 | Reemplazar el dataset simulado de la U1 por datos reales del Perú, si se publican con suficiente frecuencia | Modelos batch con señal real |
