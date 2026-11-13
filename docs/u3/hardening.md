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
| 1 | Ningún servicio declaraba si estaba listo; las dependencias esperaban solo a que el contenedor arrancara. | Los `compose` se escribieron con la configuración mínima de clase. | `healthcheck` en 8 servicios y `depends_on` con `service_healthy` (sección 2). | Pendiente: `scripts/levantar.ps1` termina con código 0 y `docker ps` muestra `(healthy)`. |
| 2 | Usuario y contraseña de Grafana escritos en `obs/compose.yml`. | Valor fijo en un archivo versionado. | Variables `${GRAFANA_ADMIN_USER:-admin}` y `${GRAFANA_ADMIN_PASSWORD:-admin}`. | Pendiente: con `$env:GRAFANA_ADMIN_PASSWORD="otra-clave"`, `docker compose -f obs/compose.yml config` muestra `otra-clave`. |
| 3 | Si Kafka no está listo cuando arranca el puente, el puente se cae y depende del reinicio automático. | `KafkaProducer(...)` se creaba sin reintentos, y Kafka está en otro proyecto de Compose. | Función `crear_productor()` con reintentos y espera creciente. | Pendiente: reiniciar el puente con Kafka detenido muestra `kafka_no_disponible` en el log y se conecta solo al volver Kafka. |
| 4 | El repositorio versionaba `artifacts/` (Parquet y modelos de la Unidad 1), pese a que el `.gitignore` decía ignorarlo. | El patrón `sesiones/*/artifacts/` no coincidía con las rutas reales (`pyspark/sesiones/AquaVigia_Producto_U1/<integrante>/artifacts/`). | Patrón `**/artifacts/` y `git rm -r --cached` de esas carpetas. | `git ls-files` no lista ninguna ruta con `/artifacts/`. |
| 5 | Una copia clonada del proyecto no podía levantar Kafka ni la observabilidad. | Los `compose` de `kafka/` y `obs/` vivían en el repositorio del curso. | `kafka/`, `obs/`, `uso-agua-usgs/` y `scripts/` dentro del repositorio del proyecto. | Pendiente: clonar el repositorio en otra carpeta y ejecutar `scripts/levantar.ps1`. |
| 6 | El puente MQTT estaba duplicado en dos carpetas. | Se copió a `pyspark/sesiones/AquaVigia_Producto_U2/puente_mqtt/` para versionarlo. | Una sola copia, en `uso-agua-usgs/`. | `git ls-files` ya no lista `puente_mqtt/`. |

## 5. Prueba de no regresión

Punto de partida guardado antes de aplicar los cambios (12-nov, 23:27): [contenedores](evidencias/docker_ps_antes.txt) y [tópicos con sus offsets](evidencias/topicos_antes.txt). En ese momento `agua-usgs-eventos-v2` tenía 3 particiones y 51 548 mensajes (20 662 / 19 896 / 10 990), `orden-eventos` 49 y `alerta-eventos` 81, los dos con 1 partición. Como Kafka no tiene volumen (sección 3), aplicar el hardening recrea el broker y estos tópicos vuelven a crearse vacíos.

La prueba de no regresión es la ejecución integrada de S13: se corre **antes** de aplicar estos cambios, se aplican, y se vuelve a correr para comparar. La tabla antes y después se completa cuando exista el cuaderno de ejecución integrada.
