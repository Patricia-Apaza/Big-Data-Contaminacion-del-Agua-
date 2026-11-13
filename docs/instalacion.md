# Instalación y ejecución

Esta página lleva el sistema completo desde un repositorio recién clonado hasta la primera ejecución integrada. Está escrita para Windows, que es donde lo probamos. Los comandos van en **CMD**, salvo donde se indica.

## 1. Requisitos

| Requisito | Versión usada | Para qué |
|---|---|---|
| Windows 10 u 11 | 11 | Sistema donde se probó |
| Docker Desktop (con WSL 2) | el instalado en la laptop del equipo | Todos los servicios corren en contenedores |
| PowerShell | 5.1 (viene con Windows) | Scripts de `scripts/` |
| Git | cualquiera reciente | Clonar el repositorio |
| Python en Windows | 3.12 | Solo para el productor del USGS (`usgs_productor.py`) |
| `paho-mqtt` | 2.1.0 | El productor publica por MQTT |
| Conexión a internet | | Descargar imágenes de Docker, el conector de Kafka para Spark y el historial del USGS |
| Clave de la API del USGS | opcional y gratuita | Sube el límite de consultas: [api.waterdata.usgs.gov/signup](https://api.waterdata.usgs.gov/signup) |

Todo lo demás (Kafka 4.3.1, Spark 4.2.0, Java 21, Prometheus, Grafana, Mosquitto) viene dentro de las imágenes, con la versión fijada en los archivos `compose.yml` y `Dockerfile`.

Puertos que usa el sistema en la máquina: `49092`, `48085`, `49308`, `4488`, `4042`, `49090`, `49091`, `43000` y `41883`. Deben estar libres.

!!! warning "Si ya tiene el laboratorio del curso"
    Los contenedores se llaman igual que los del repositorio del curso (`lambda26-kafka`, `lambda26-grafana`...) y usan los mismos nombres de proyecto de Compose. Al levantar este sistema, Compose **reemplaza** esos contenedores por los de este repositorio.

## 2. Clonar y configurar

```
git clone https://github.com/Patricia-Apaza/Big-Data-Contaminacion-del-Agua-.git
cd Big-Data-Contaminacion-del-Agua-
copy pyspark\.env.example pyspark\.env
copy obs\.env.example obs\.env
```

Los archivos `.env` no se versionan. Ahí se cambian el token de Jupyter y la clave de Grafana. Si no se crean, el sistema arranca con los valores por defecto (`sintoken`, `admin`/`admin`), como se declara en los [riesgos aceptados](u3/hardening.md#3-riesgos-aceptados).

## 3. Levantar los servicios

```
powershell -ExecutionPolicy Bypass -File scripts\levantar.ps1
```

El script levanta, en este orden y esperando a que cada parte esté **lista** (`healthcheck` + `docker compose up -d --wait`):

1. Kafka, Kafka UI y Kafka Exporter.
2. Los tres tópicos: `agua-usgs-eventos-v2` (3 particiones), `orden-eventos` y `alerta-eventos` (1 partición cada uno).
3. PySpark con Jupyter (se construye la imagen la primera vez).
4. Prometheus, Pushgateway y Grafana.
5. Mosquitto (MQTT) y el puente MQTT a Kafka.

Al final muestra `docker ps`. Todos deben decir `(healthy)`, salvo Kafka UI, que no tiene comprobación de salud.

| Servicio | Dirección |
|---|---|
| Jupyter | http://localhost:4488 (token: `sintoken` o el de `pyspark\.env`) |
| Spark UI | http://localhost:4042 (solo mientras corre un cuaderno) |
| Kafka UI | http://localhost:48085 |
| Prometheus | http://localhost:49090 |
| Pushgateway | http://localhost:49091 |
| Grafana | http://localhost:43000 (usuario y clave de `obs\.env`; por defecto `admin`/`admin`) |
| MQTT, para el productor | `localhost:41883` |

## 4. Preparar el productor del USGS

El productor corre en Windows, fuera de Docker, como lo haría un dispositivo de campo.

```
py -m pip install paho-mqtt==2.1.0
cd pyspark\sesiones\AquaVigia_Producto_U2\patricia
```

Si tiene clave de la API, póngala **solo en la terminal** (no en un archivo):

```
set USGS_API_KEY=su-clave
```

## 5. Generar los artefactos (una sola vez)

Los datos procesados y los modelos entrenados no se versionan (`artifacts/` está en `.gitignore`). Se generan con los cuadernos, en este orden. Abra Jupyter y ejecute cada cuaderno con **Kernel → Restart Kernel and Run All Cells**, **uno a la vez**: dos kernels que arrancan juntos se bloquean al descargar el conector de Kafka.

### 5.1 Unidad 1: Parquet gold y modelo batch

| Integrante | Cuaderno | Genera |
|---|---|---|
| Patricia | `AquaVigia_Producto_U1/patricia/04_producto_u1_patricia.ipynb` | `artifacts/mediciones_particionado` y `artifacts/modelo_alerta_siguiente_mes` |
| Jhenderson | `AquaVigia_Producto_U1/jhenderson/04_producto_u1_jhenderson.ipynb` | `artifacts/mediciones_particionado` y `artifacts/modelo_ph_siguiente_mes` |

Los datos de entrada (`data/estaciones_agua.csv` y `data/mediciones_calidad_agua.parquet`) sí están en el repositorio. Cada cuaderno debe terminar con `Verificado: 1,245,817 filas, ida y vuelta sin perdida.`

### 5.2 Historial del USGS (90 días)

Desde la carpeta del productor (paso 4):

```
py usgs_productor.py historial --estaciones USGS-01491000,USGS-01579550,USGS-01589290,USGS-01594441,USGS-01640152,USGS-0164578734,USGS-0164579522,USGS-01646500,USGS-01649190,USGS-01649500,USGS-01650800,USGS-02054550,USGS-02074556 --dias 90 --variables temperatura_c,oxigeno_mgl,ph,turbidez_fnu --salida-archivo ..\data\historial.jsonl
```

- Descarga las lecturas de las 13 estaciones que usa el proyecto. Nuestra descarga del 30 de octubre de 2026 dio 1 309 777 lecturas (unos 430 MB).
- El archivo queda en `pyspark/sesiones/AquaVigia_Producto_U2/data/historial.jsonl` y no se versiona por su tamaño.
- El productor **agrega** al archivo si ya existe. Para descargar de nuevo, borre antes el archivo.
- Una descarga hecha otro día cubre otros 90 días, así que los modelos que salgan serán algo distintos de los nuestros.

### 5.3 Unidad 2: modelos de series de tiempo

| Integrante | Cuaderno | Genera |
|---|---|---|
| Patricia | `AquaVigia_Producto_U2/patricia/12b_modelo_usgs_patricia.ipynb` | `artifacts/modelos_patricia` (modelos de temperatura y oxígeno, `meta.json` con RMSE y umbrales por estación) |
| Jhenderson | `AquaVigia_Producto_U2/jhenderson/12b_modelo_usgs_jhenderson.ipynb` | `artifacts/modelos_jhenderson` (modelos de pH y turbidez y su `meta.json`) |

## 6. Primera ejecución integrada

Con los artefactos listos, siga [Uso del sistema](uso.md#1-ejecucion-integrada-recomendada). Es la prueba de que todo quedó bien instalado: si falta algo, la sección 1 del cuaderno lo dice por nombre.

## 7. Apagar y volver a encender

- Apagar: `docker stop` de los contenedores, o cerrar Docker Desktop. Los artefactos quedan en disco.
- Volver a encender: `scripts\levantar.ps1` otra vez.
- **Kafka no tiene volumen.** Si su contenedor se **recrea** (no solo se reinicia), los tópicos quedan vacíos. El script los vuelve a crear. Para tener datos, se reenvían con el productor (`reproducir`). Es un [riesgo aceptado](u3/hardening.md#3-riesgos-aceptados).

## 8. Problemas frecuentes

| Síntoma | Causa | Qué hacer |
|---|---|---|
| `can't open file ... usgs_productor.py` | La terminal no está en la carpeta del productor | `cd pyspark\sesiones\AquaVigia_Producto_U2\patricia` |
| Run All no muestra salida durante minutos | Dos cuadernos arrancaron a la vez | Apagar los dos kernels (pestaña Running) y ejecutar uno solo |
| `FALLA Kafka: topico agua-usgs-eventos-v2 con datos` | Kafka se recreó y el tópico está vacío | Enviar datos con el productor (ver [Uso](uso.md)) |
| `FALLA Completitud: 1 245 817 filas de la U1` | Faltan archivos del Parquet gold | Volver a ejecutar el `04` del integrante ([caso real](u3/integracion.md#5-bitacora-de-integracion)) |
| Grafana muestra "No data" | No hay ejecuciones recientes o Prometheus aún no hizo su lectura (cada 15 s) | Ejecutar el cuaderno 13 y esperar unos segundos |
