# Uso del sistema

Con los servicios arriba y los artefactos generados ([Instalación](instalacion.md)), el sistema se usa de tres formas. Para revisar o sustentar el producto, la primera es la que integra todo.

## 1. Ejecución integrada (recomendada)

Un cuaderno por integrante ejecuta **batch, streaming, modelo, eventos de negocio y observabilidad juntos**, con compuertas que detienen la ejecución si algo falla, y deja un manifiesto de la corrida.

| Integrante | Cuaderno |
|---|---|
| Patricia | `AquaVigia_Producto_U3/patricia/13_integracion_patricia.ipynb` |
| Jhenderson | `AquaVigia_Producto_U3/jhenderson/13_integracion_jhenderson.ipynb` |

Pasos:

1. **Si Kafka está vacío** (recién levantado), envíe unos datos para que pase la compuerta "tópico con datos". Unos 300 eventos, 15 segundos. Desde la carpeta del productor:
   ```
   py usgs_productor.py reproducir --archivo ..\data\historial.jsonl --desde 2026-10-16T20:00 --hasta 2026-10-16T20:30 --velocidad 20 --salida mqtt
   ```
2. En Jupyter abra el cuaderno 13 y ejecute **Kernel → Restart Kernel and Run All Cells**.
3. Cuando el cuaderno imprima `Streaming 240 s ... (el productor debe estar enviando)`, arranque el productor. El streaming lee solo lo nuevo, así que lo enviado antes no cuenta:
   ```
   py usgs_productor.py reproducir --archivo ..\data\historial.jsonl --desde 2026-10-16T22:00 --hasta 2026-10-17T04:00 --velocidad 10 --salida mqtt
   ```
   Ese tramo contiene un episodio real en el que el riesgo supera 0,5. `--velocidad` es lecturas por segundo, no un multiplicador.
4. El cuaderno termina solo, en unos 4 minutos y medio. Al terminar, apague el kernel antes de abrir el del otro integrante.

Qué hace cada sección del cuaderno:

| Sección | Qué hace | Si falla |
|---|---|---|
| 1. Compuertas previas | Comprueba que existen el Parquet y los modelos, que Kafka tiene datos, y que MQTT, Pushgateway, Prometheus y Grafana responden | Se detiene y dice qué falta |
| 2. Batch | Lee el Parquet gold de la U1 y aplica compuertas de existencia, completitud, rango y plausibilidad | Se detiene antes del streaming |
| 3. Streaming | 240 s de Spark Structured Streaming: valida, deduplica dentro del watermark, pronostica a 1 hora, calcula el riesgo y publica eventos de negocio | |
| 3.1 Compuertas de salida | El stream entregó filas y los eventos emitidos están de verdad en Kafka. Compara el error en vivo del modelo con el de repetir el último valor | Se detiene; una persistencia que gana al modelo solo **advierte** |
| 4. KPI de tiempo | Latencia de punta a punta y duración del micro-lote en p50 y p95 | Se detiene si el p50 no es plausible; un p95 alto solo advierte |
| 5. Observabilidad | Publica en Pushgateway y confirma que Prometheus devuelve **el valor recién publicado** | Se detiene si no coincide |
| 6. Manifiesto | Escribe `artifacts/manifiestos/manifiesto_<integrante>_<runId>.json` y compara con la corrida anterior | |

Resultado: en Grafana (http://localhost:43000), carpeta **AquaVigia**, tablero **AquaVigia U3: sistema integrado (Equipo 02)**. La fila "Ejecución integrada U3" de cada integrante muestra la antigüedad de la última corrida, las latencias, las compuertas que pasaron y el RMSE en vivo.

## 2. Pipeline en tiempo real continuo (Unidad 2)

Para ver el pipeline corriendo de forma continua, con lag, throughput, pronóstico y riesgo actualizándose en Grafana:

| Integrante | Cuaderno |
|---|---|
| Patricia | `AquaVigia_Producto_U2/patricia/12c_negocio_usgs_patricia_v3.ipynb` |
| Jhenderson | `AquaVigia_Producto_U2/jhenderson/12c_negocio_usgs_jhenderson_v3.ipynb` |

En la primera celda, `MODO = "reproduccion"` lee el tópico desde el principio y `MODO = "vivo"` solo lo nuevo. `DURACION_MIN` fija cuánto corre. Las filas de cada integrante del mismo tablero de Grafana muestran el resultado.

Con datos en vivo del USGS en lugar del historial:

```
py usgs_productor.py publicar --estaciones USGS-01491000,USGS-01579550,USGS-01589290,USGS-01594441,USGS-01640152,USGS-0164578734,USGS-0164579522,USGS-01646500,USGS-01649190,USGS-01649500,USGS-01650800,USGS-02054550,USGS-02074556 --cada 120 --salida mqtt
```

El USGS publica sus lecturas por tandas, con unos 40 a 54 minutos de retraso, así que en vivo los eventos llegan en bloques y no cada 5 minutos.

## 3. Cuadernos de cada tema

Para revisar un tema concreto de las unidades anteriores:

| Cuaderno | Tema |
|---|---|
| `AquaVigia_Producto_U1/<integrante>/04_producto_u1_<integrante>.ipynb` | Batch: transformaciones, calidad, Parquet particionado y modelo MLlib |
| `AquaVigia_Producto_U2/<integrante>/12a_streaming_usgs_<integrante>_v2.ipynb` | Ingesta verificada, ventana, watermark medido, checkpoint y dato tardío |
| `AquaVigia_Producto_U2/<integrante>/12b_modelo_usgs_<integrante>.ipynb` | Modelo de series de tiempo validado contra la persistencia y la media |
| `AquaVigia_Producto_U2/<integrante>/12d_ingesta_semantica_<integrante>.ipynb` | Contrato versionable, partición por key, semántica al menos una vez y ventanas |
| `AquaVigia_Producto_U2/<integrante>/12e_evaluacion_riesgo_<integrante>.ipynb` | Curva ROC del riesgo y curva de aprendizaje |

## 4. Comprobar la salud del sistema

```
docker ps
powershell -ExecutionPolicy Bypass -File scripts\pruebas_hardening.ps1
```

El segundo comando repite las pruebas de la revisión técnica: estado de los contenedores, tópicos, variables de Grafana y reconexión del puente cuando Kafka se cae. Deja la salida en `docs/u3/evidencias/`. **Detiene Kafka unos segundos a propósito**, así que no lo ejecute durante una corrida.
