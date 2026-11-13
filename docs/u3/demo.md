# Guion de demostración

Demostración de punta a punta del sistema integrado (S14, 3.8). Está pensada para los **5 minutos de demostración por integrante** de S15 y usa la ejecución integrada (cuaderno 13), que en una sola corrida pasa por batch, streaming, modelo, eventos de negocio y observabilidad. Las cifras que se mencionan están en [Pruebas y evidencias, sección 5](../evidencias.md#5-metricas-para-la-demostracion), con su origen.

Cada integrante muestra **su propia corrida**:

| | Patricia | Jhenderson |
|---|---|---|
| Cuaderno | `AquaVigia_Producto_U3/patricia/13_integracion_patricia.ipynb` | `AquaVigia_Producto_U3/jhenderson/13_integracion_jhenderson.ipynb` |
| Variables | temperatura y oxígeno disuelto | pH y turbidez |
| Tópico de negocio | `orden-eventos` | `alerta-eventos` |

## 1. Antes de entrar (15 minutos antes)

| # | Paso | Comando o URL | Debe verse |
|---|---|---|---|
| 1 | Levantar el sistema | `powershell -ExecutionPolicy Bypass -File scripts\levantar.ps1` | Termina sin `Fallo` |
| 2 | Comprobar la salud | `docker ps` | 8 servicios con `(healthy)` |
| 3 | Datos en Kafka (si Kafka se recreó) | En `pyspark\sesiones\AquaVigia_Producto_U2\patricia`: `py usgs_productor.py reproducir --archivo ..\data\historial.jsonl --desde 2026-10-16T20:00 --hasta 2026-10-16T20:30 --velocidad 20 --salida mqtt` | `enviados` sube hasta unos 300 |
| 4 | Ningún kernel abierto | Jupyter → pestaña **Running** → Shut Down All | Lista vacía |
| 5 | Pestañas abiertas en el navegador | Jupyter `http://localhost:4488`, Grafana `http://localhost:43000/d/aquavigia-u3`, Kafka UI `http://localhost:48085`, Prometheus `http://localhost:49090` | Las cuatro cargan |
| 6 | Terminal del productor lista | En la carpeta del productor, con el comando del minuto 1 **escrito sin presionar Enter** | |
| 7 | Plan alternativo a mano | Carpeta con las capturas de la sección 3 y el sitio de documentación abierto | |

## 2. Guion (5 minutos)

| Min. | Qué se muestra | Comando o URL | Qué debe verse | Plan alternativo |
|---|---|---|---|---|
| 0:00-0:30 | Entorno sano: cada servicio declara si está **listo**, no solo si arrancó | `docker ps` | 8 servicios `(healthy)` | Captura 1 (con fecha y hora) |
| 0:30-1:00 | Lanzar la ejecución integrada. **Sección 1: compuertas previas** | Jupyter → cuaderno 13 → **Restart Kernel and Run All Cells** | 9 líneas `OK` (Parquet, modelos, Kafka, MQTT, Pushgateway, Prometheus, Grafana) | Si una falla, el cuaderno dice cuál: se corrige (por ejemplo, el paso 3 de la sección 1). Si no hay tiempo: captura 2 |
| 1:00-1:30 | **Sección 2: batch de la U1 con compuertas de calidad** | Misma salida | `OK Completitud: 1 245 817 filas`, 8 regiones, p50 y p95 de cada variable | Captura 3. **Caso de error**: [corrida detenida por Parquet incompleto](https://github.com/Patricia-Apaza/Big-Data-Contaminacion-del-Agua-/blob/main/docs/u3/evidencias/jhenderson_corrida_fallida_20261113T110211Z.txt) |
| 1:30 | Al aparecer `Streaming 240 s ...`, **arrancar el productor** | `py usgs_productor.py reproducir --archivo ..\data\historial.jsonl --desde 2026-10-16T22:00 --hasta 2026-10-17T04:00 --velocidad 10 --salida mqtt` | El contador `enviados` sube | Si MQTT o el puente fallan: el mismo comando con `--salida kafka`, que escribe directo en `localhost:49092` (requiere `py -m pip install kafka-python==2.0.2`) |
| 1:30-3:30 | **Ingesta en vivo** mientras corre el streaming | Grafana → fila "Infraestructura compartida": *Ingesta a Kafka* y *Mensajes por partición*. Kafka UI → Topics → `agua-usgs-eventos-v2` | La tasa de ingesta sube y los mensajes crecen en las 3 particiones | Captura 4 |
| 3:30-5:30 | **Decisión**: el evento de negocio aparece en Kafka | Kafka UI → Topics → `orden-eventos` o `alerta-eventos` → Messages | Un JSON con `valor_actual`, `pronostico_1h`, `riesgo` ≥ 0,5 y `evento_id` | Captura 5 |
| 5:30-6:00 | **Secciones 3.1 a 6**: compuertas de salida, KPI de tiempo, Prometheus y manifiesto | Salida del cuaderno | `Los N eventos emitidos están en Kafka`; latencia p50 de unos 7 s y p95 de unos 11 s; 15 `OK` en Prometheus; `Manifiesto escrito en ...` y la tabla que compara con la corrida anterior | Captura 6 y el [manifiesto de una corrida anterior](https://github.com/Patricia-Apaza/Big-Data-Contaminacion-del-Agua-/tree/main/docs/u3/evidencias/manifiestos) |
| Cierre | **Observabilidad de la corrida** | Grafana → fila "Ejecución integrada U3" del integrante | *Antigüedad de la última ejecución* en segundos, compuertas 21 y el RMSE en vivo | Captura 7 |

!!! warning "La demostración dura unos 6 minutos de reloj"
    El cuaderno tarda unos 4 minutos y medio en terminar: 4 minutos son de streaming. Si el tiempo es estricto, lance el cuaderno **al empezar la última lámina antes de la demo**, para que al abrir la demostración ya esté en la sección 2.

### Qué decir en cada paso

| Paso | Idea clave |
|---|---|
| `docker ps` | "Healthy quiere decir listo, no solo encendido. `levantar.ps1` espera a que cada servicio lo esté antes de levantar el siguiente." |
| Compuertas previas | "Si falta algo, el cuaderno se detiene en el primer segundo y dice qué falta, en vez de fallar minutos después dentro de Spark." |
| Batch | "La completitud no es decorativa: detuvo una corrida real con un tercio de las filas faltantes. Las demás compuertas pasaban." |
| Streaming | "El modelo se entrenó por lotes con 90 días de historial y aquí se aplica sin reentrenar, con una sola lectura, porque cada evento trae su contexto." |
| Evento de negocio | "Cuando el riesgo de salir del rango pasa de 0,5, el sistema publica una orden o una alerta. Su id es determinista: si el lote se reprocesa, no se duplica." |
| Prometheus | "No basta con que la métrica exista: el cuaderno comprueba que Prometheus devuelve el valor recién publicado, porque un valor viejo en el Pushgateway parece vivo." |
| Manifiesto | "Dos corridas no procesan las mismas lecturas; lo que se compara es que pasen las mismas compuertas." |

## 3. Capturas del plan alternativo

Se toman en un ensayo completo, **con la fecha y la hora del sistema visibles** (la barra de tareas de Windows). Una captura sin fecha y hora no demuestra nada (S14, 3.8).

| # | Captura | Dónde |
|---|---|---|
| 1 | `docker ps` con los servicios `(healthy)` | CMD |
| 2 | Sección 1 del cuaderno 13: 9 `OK` | Jupyter |
| 3 | Sección 2: compuertas de calidad y filas | Jupyter |
| 4 | Panel de ingesta a Kafka durante el streaming | Grafana |
| 5 | Un evento de negocio abierto en Kafka UI | Kafka UI |
| 6 | Secciones 4 a 6: KPI de tiempo, 15 `OK` en Prometheus y manifiesto | Jupyter |
| 7 | Fila "Ejecución integrada U3" con la antigüedad en segundos | Grafana |

## 4. Si algo falla durante la demostración

| Falla | Qué se ve | Qué hacer |
|---|---|---|
| Un servicio no está `healthy` | `docker ps` muestra `starting` o `unhealthy` | Esperar 30 s y volver a mirar. Si sigue igual, `scripts\levantar.ps1` otra vez |
| `FALLA Kafka: topico ... con datos` | Compuerta previa | Paso 3 de la sección 1 (unos 15 s) y volver a ejecutar |
| El cuaderno no muestra salida | Run All sin avanzar | Dos kernels abiertos: Running → Shut Down All y volver a ejecutar |
| `El stream no entrego ninguna fila` | Error en la sección 3.1 | El productor no envió durante el streaming. Volver a ejecutar y arrancarlo al ver `Streaming 240 s` |
| Grafana sin datos | "No data" | Prometheus lee cada 15 s: esperar y recargar. Si no, captura 7 |
| Nada funciona | | Capturas 1 a 7, [manifiestos](https://github.com/Patricia-Apaza/Big-Data-Contaminacion-del-Agua-/tree/main/docs/u3/evidencias/manifiestos) y [ejecución integrada](integracion.md#4-corridas-y-comparacion-de-manifiestos) en el sitio publicado |
