# Revisión técnica y hardening (S14)

Esta página registra la revisión de configuración del sistema AquaVigía, los fallos corregidos, los riesgos que aceptamos y la bitácora de la revisión. Sigue la guía S14 de `lambda26` (Tablas 2, 5 y 9).

## 1. Dónde vive la configuración

Hasta la Unidad 2, los archivos de Docker Compose estaban en nuestra copia del repositorio del curso, no en el repositorio del proyecto. Una persona que clonara el proyecto no podía levantar Kafka ni la observabilidad. Desde esta revisión, todo está en este repositorio:

| Carpeta | Qué levanta | Contenedores |
|---|---|---|
| `kafka/` | Broker Kafka, Kafka UI y Kafka Exporter | `lambda26-kafka`, `lambda26-kafka-ui`, `lambda26-kafka-exporter` |
| `pyspark/` | PySpark 4.2 con Jupyter | `agua-pyspark` |
| `obs/` | Prometheus, Pushgateway y Grafana | `lambda26-prometheus`, `lambda26-pushgateway`, `lambda26-grafana` |
| `uso-agua-usgs/` | Mosquitto (MQTT) y el puente MQTT a Kafka | `lambda26-mosquitto`, `lambda26-puente-usgs` |
| `scripts/` | `levantar.ps1`, `crear_topicos.ps1`, `probar_salud.ps1` | |

## 2. Lista de comprobación de configuración

Salida de los comandos de lectura: [antes](evidencias/revision_config_antes.txt) y [después](evidencias/revision_config_despues.txt).

| Punto | Antes | Acción | Después |
|---|---|---|---|
| Versión de las imágenes | Cumple: las 7 imágenes tienen versión exacta (`apache/kafka:4.3.1`, `prom/prometheus:v3.14.0`, `grafana/grafana:11.4.0`, `eclipse-mosquitto:2.0.20`...). Las dos imágenes propias se construyen con `build:` y dependencias fijadas (`pyspark==4.2.0`, `kafka-python==2.0.2`, `paho-mqtt==2.1.0`). | Ninguna. | Cumple. La base del puente (`python:3.11-slim`) fija la versión menor, no el parche: riesgo aceptado (sección 3). |
| Reinicio | Cumple: `restart: unless-stopped` en los 9 servicios. | Ninguna. | Cumple. |
| Salud | **Falla**: 0 `healthcheck` en los 4 archivos. | `healthcheck` en Kafka, Kafka Exporter, Prometheus, Pushgateway, Grafana, Mosquitto, el puente y Jupyter, cada comando probado antes a mano (`scripts/probar_salud.ps1`). | 8 servicios con `healthcheck`. Los 8 comandos, probados a mano en los contenedores en ejecución el 12-nov a las 23:27, terminaron con código 0 ([salida](evidencias/healthchecks_prueba_manual.txt)). |
| Orden de arranque | **Falla**: `depends_on: - kafka`, `- prometheus` y `- mosquitto` solo esperan a que el contenedor **arranque**. | `depends_on` con `condition: service_healthy`; el sistema se levanta con `up -d --wait` (`scripts/levantar.ps1`). Kafka está en otro proyecto de Compose que el puente, así que el puente además reintenta la conexión a Kafka. | 4 dependencias esperan a que su servicio esté **listo**. |
| Secretos | **Falla**: `GF_SECURITY_ADMIN_USER: admin` y `GF_SECURITY_ADMIN_PASSWORD: admin` escritos en `obs/compose.yml`. El token de Jupyter ya se leía de `pyspark/.env`. La clave de la API del USGS ya se leía de la variable `USGS_API_KEY`. | `${GRAFANA_ADMIN_USER:-admin}` y `${GRAFANA_ADMIN_PASSWORD:-admin}`, con `obs/.env.example`. | Ninguna credencial escrita en un archivo versionado. |
| Superficie expuesta | Revisar: se publican 9 puertos (`49092`, `48085`, `49308`, `49090`, `49091`, `43000`, `4488`, `4042`, `41883`). | Se registra como riesgo aceptado. | Riesgo aceptado (sección 3). |

## 3. Riesgos aceptados

| Riesgo | Por qué se acepta aquí | Qué cambiaría en producción |
|---|---|---|
| Kafka sin volumen: recrear su contenedor vacía los tópicos. | Es un laboratorio: los tópicos se recrean con `scripts/crear_topicos.ps1` y el productor puede reenviar el historial real (`reproducir`). | Volumen persistente para los datos del broker y más de un broker con replicación. |
| Kafka y Mosquitto sin autenticación ni cifrado (`PLAINTEXT`, `allow_anonymous true`). | La red `lambda26-kafka_default` es local y el único dispositivo es nuestro productor. | TLS y autenticación en Kafka (SASL) y en Mosquitto (usuario por dispositivo y ACL por tópico). |
| Grafana con `admin`/`admin` por defecto. | El sistema debe arrancar sin configuración previa. | Contraseña propia desde el primer arranque en `obs/.env`, que no se versiona. |
| Jupyter con el token `sintoken` y los 9 puertos publicados al equipo. | Herramientas de trabajo locales que se abren desde el navegador. | Publicar solo el tablero; el resto, dentro de la red y detrás de autenticación. |
| El Pushgateway conserva el último valor: un job muerto parece vivo. | Es el mecanismo para un job que no expone `/metrics`. Lo mitigamos con el panel "Segundos desde el último lote". | Exponer `/metrics` desde un servicio de streaming que corra con `spark-submit`. |
| La base del puente (`python:3.11-slim`) no fija el parche. | Cambios de parche de Python no afectan a un puente de 80 líneas. | Fijar la imagen por *digest*. |
| La clave de la API del USGS vive en una variable de entorno de la terminal. | Nunca se escribe en un archivo del repositorio. | Un gestor de secretos. |

## 4. Bitácora de la revisión

| # | Hallazgo | Causa | Corrección | Prueba de corrección |
|---|---|---|---|---|
| 1 | Ningún servicio declaraba si estaba listo; las dependencias esperaban solo a que el contenedor arrancara. | Los `compose` se escribieron con la configuración mínima de clase. | `healthcheck` en 8 servicios y `depends_on` con `service_healthy` (sección 2). 13/11, 09:37: `scripts/levantar.ps1` terminó sin error y cada paso esperó al anterior. `docker ps` muestra `(healthy)` en los 8 servicios con healthcheck ([evidencia](evidencias/docker_ps_despues.txt)). `kafka-ui` no tiene healthcheck y Compose no lo recreó, así que sigue como `Up`. |
| 2 | Usuario y contraseña de Grafana escritos en `obs/compose.yml`. | Valor fijo en un archivo versionado. | Variables `${GRAFANA_ADMIN_USER:-admin}` y `${GRAFANA_ADMIN_PASSWORD:-admin}`. 13/11: sin variable, `config` muestra `admin`; con `$env:GRAFANA_ADMIN_PASSWORD='prueba-variable-s14'` muestra `prueba-variable-s14` ([evidencia](evidencias/grafana_variable_prueba.txt)). La prueba no cambió la clave real de Grafana. |
| 3 | Si Kafka no está listo cuando arranca el puente, el puente se cae y depende del reinicio automático. | `KafkaProducer(...)` se creaba sin reintentos, y Kafka está en otro proyecto de Compose. | Función `crear_productor()` con reintentos y espera creciente. 13/11, 09:38 a 09:40: con Kafka detenido, el puente registró 3 intentos `kafka_no_disponible` (`NoBrokersAvailable`) y no se cayó. Su estado quedó en `starting`, dentro del período de arranque del healthcheck. Al volver Kafka registró `starting` y `connected`, y pasó a `healthy` sin intervención ([evidencia](evidencias/puente_reintento_prueba.txt)). |
| 4 | El repositorio versionaba `artifacts/` (Parquet y modelos de la Unidad 1), pese a que el `.gitignore` decía ignorarlo. | El patrón `sesiones/*/artifacts/` no coincidía con las rutas reales (`pyspark/sesiones/AquaVigia_Producto_U1/<integrante>/artifacts/`). | Patrón `**/artifacts/` y `git rm -r --cached` de esas carpetas. | `git ls-files` no lista ninguna ruta con `/artifacts/`. |
| 5 | Una copia clonada del proyecto no podía levantar Kafka ni la observabilidad. | Los `compose` de `kafka/` y `obs/` vivían en el repositorio del curso. | `kafka/`, `obs/`, `uso-agua-usgs/` y `scripts/` dentro del repositorio del proyecto. | Pendiente: clonar el repositorio en otra carpeta y ejecutar `scripts/levantar.ps1`. |
| 6 | El puente MQTT estaba duplicado en dos carpetas. | Se copió a `pyspark/sesiones/AquaVigia_Producto_U2/puente_mqtt/` para versionarlo. | Una sola copia, en `uso-agua-usgs/`. | `git ls-files` ya no lista `puente_mqtt/`. |
| 7 | Los cuadernos de la Unidad 2 (`12a`, `12b`, `12c` y `12e` de los dos integrantes) usaban rutas del repositorio del curso (`/opt/s12-u2-usgs/...`). En una copia limpia del proyecto, el `12b` no encontraba el historial ni podía guardar los modelos donde los busca el cuaderno `13`. | Los cuadernos se escribieron en el repositorio del curso y se copiaron al proyecto sin cambiar las rutas. Los modelos del `13` se habían copiado a mano. | `BASE = "/opt/AquaVigia_Producto_U2"` y `ARTIFACTS = f"{BASE}/<integrante>/artifacts"`. El historial va en `AquaVigia_Producto_U2/data/historial.jsonl` ([Instalación](../instalacion.md#52-historial-del-usgs-90-dias)). | Pendiente: en una copia clonada, el `12b` de cada integrante genera `modelos_<integrante>` y el `13` pasa sus compuertas previas (prueba de la fila 5). |

## 5. Prueba de no regresión

Punto de partida guardado antes de aplicar los cambios (12-nov, 23:27): [contenedores](evidencias/docker_ps_antes.txt) y [tópicos con sus offsets](evidencias/topicos_antes.txt). En ese momento `agua-usgs-eventos-v2` tenía 3 particiones y 51 548 mensajes (20 662 / 19 896 / 10 990), `orden-eventos` 49 y `alerta-eventos` 81, los dos con 1 partición. Como Kafka no tiene volumen (sección 3), aplicar el hardening recrea el broker y estos tópicos vuelven a crearse vacíos.

La prueba de no regresión es la ejecución integrada de S13: se corre **antes** de aplicar estos cambios, se aplican, y se vuelve a correr para comparar.

**Aplicación (13/11, 09:37).** `scripts/levantar.ps1` recreó Kafka, y `scripts/crear_topicos.ps1` volvió a crear los 3 tópicos con las mismas particiones (3, 1 y 1), en 0 mensajes ([evidencia](evidencias/topicos_despues.txt)). Era lo esperado según la sección 3, porque Kafka no tiene volumen. Después se ejecutó `scripts/pruebas_hardening.ps1`, que cierra las filas 1 a 3 de la bitácora.

**Comparación antes y después.** Las corridas "antes" son las de la ejecución integrada con la configuración previa ([integracion.md](integracion.md), sección 4): Patricia 20261113T104634Z y Jhenderson 20261113T112336Z. Las corridas "después" usan la misma ventana del episodio (16/10 22:00 a 17/10 04:00 UTC, 10 lecturas/s).

| Indicador | Patricia antes | Patricia después | Jhenderson antes | Jhenderson después |
|---|---|---|---|---|
| runId | 20261113T104634Z | 20261113T163113Z | 20261113T112336Z | 20261113T163824Z |
| Compuertas que pasan | 21 de 21 | 21 de 21 | 21 de 21 | 21 de 21 |
| Filas batch U1 | 1 245 817 | 1 245 817 | 1 245 817 | 1 245 817 |
| Pronósticos | 971 | 1 015 | 1 043 | 1 077 |
| Eventos de negocio (en Kafka) | 4 (4) | 4 (4) | 15 (15) | 16 (16) |
| Riesgo máximo | 0,745 | 0,745 | 1,000 | 1,000 |
| Latencia p50 / p95 | 7,4 s / 11,9 s | 6,7 s / 11,3 s | 7,5 s / 11,9 s | 7,0 s / 11,4 s |
| Micro-lote p50 / p95 | 2 883 / 3 642 ms | 2 034 / 2 652 ms | 2 974 / 3 258 ms | 2 142 / 2 835 ms |
| RMSE vivo del modelo | temp. 0,1127; oxíg. 0,1210 | temp. 0,1131; oxíg. 0,1260 | pH 0,0570; turb. 6,2517 | pH 0,0570; turb. 6,1172 |
| Métricas confirmadas en Prometheus | 15 de 15 | 15 de 15 | 15 de 15 | 15 de 15 |

**Resultado: sin regresión.**

- Pasan las mismas compuertas.
- Los eventos emitidos coinciden con los guardados en Kafka.
- El riesgo máximo es idéntico.
- El error del modelo en vivo varía en el tercer decimal: las lecturas no son exactamente las mismas, porque el streaming lee solo lo nuevo y el productor se arranca a mano.

Los micro-lotes fueron más rápidos después del cambio, pero no se atribuye al hardening: los healthchecks no actúan sobre Spark. La diferencia coincide con que Kafka arrancó con los tópicos vacíos (antes tenían unos 66 000 mensajes acumulados) y los contenedores eran nuevos.

Las dos corridas "después" imprimieron un error largo al detener el streaming. No afecta a los resultados y se corrigió en el cuaderno ([integracion.md](integracion.md), bitácora, fila 4). Manifiestos en [`evidencias/manifiestos/`](https://github.com/Patricia-Apaza/Big-Data-Contaminacion-del-Agua-/tree/main/docs/u3/evidencias/manifiestos).
