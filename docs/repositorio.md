# Repositorio y estándares

Repositorio: [github.com/Patricia-Apaza/Big-Data-Contaminacion-del-Agua-](https://github.com/Patricia-Apaza/Big-Data-Contaminacion-del-Agua-), rama `main`.

## Topics académicos

La guía del Proyecto Sello exige topics que identifiquen campus, semestre, línea, tipo de proyecto, curso, sección y grupo, según el estándar transversal de topics para repositorios académicos:

| Topic | Identifica |
|---|---|
| `campus-juliaca` | campus |
| `semestre-2026-2` | semestre |
| `linea-cdia` | línea de Ciencia de Datos e IA |
| `tipo-ps` | tipo de proyecto: Proyecto Sello |
| `bigdata` | curso |
| `seccion-<sección>` | sección |
| `grupo-02-aquavigia` | grupo y nombre del proyecto |

Además, según el Brief, el repositorio tiene topics técnicos: `big-data`, `grafana`, `kafka`, `prometheus`, `pyspark`, `spark-structured-streaming`, `time-series`, `upeu` y `usgs`.

## Estructura

```
.
├── kafka/                  Kafka, Kafka UI y Kafka Exporter (Compose)
├── obs/                    Prometheus, Pushgateway, Grafana y su tablero
├── uso-agua-usgs/          Mosquitto y puente MQTT a Kafka
├── pyspark/                Imagen de PySpark + Jupyter
│   └── sesiones/           Se monta en /opt dentro del contenedor
│       ├── AquaVigia_Producto_U1/<integrante>/   Unidad 1 (batch)
│       ├── AquaVigia_Producto_U2/<integrante>/   Unidad 2 (tiempo real) y productor
│       └── AquaVigia_Producto_U3/<integrante>/   Unidad 3 (ejecución integrada)
├── scripts/                Levantar el sistema, crear tópicos, probar la salud
├── docs/                   Esta documentación y las evidencias de la U3
└── mkdocs.yml
```

Cada unidad tiene una carpeta por integrante, como pide la guía del proyecto.

## Qué no se versiona y por qué

| Patrón en `.gitignore` | Qué es | Cómo se obtiene |
|---|---|---|
| `**/.env` | Credenciales reales | Se copia de `.env.example` |
| `**/artifacts/` | Parquet, modelos, checkpoints y manifiestos | Ejecutando los cuadernos ([Instalación](instalacion.md#5-generar-los-artefactos-una-sola-vez)) |
| `**/checkpoint*/`, `**/checkpoints/` | Estado de Spark Streaming | Se crea al correr el stream |
| `**/historial*.jsonl` | Historial del USGS, unos 430 MB | `usgs_productor.py historial` |
| `site/` | Sitio MkDocs generado | `mkdocs build` o `mkdocs gh-deploy` |

Hasta la Unidad 2, `artifacts/` de la Unidad 1 sí estaba versionado porque el patrón del `.gitignore` no coincidía con la ruta real. Se corrigió en la Unidad 3 ([bitácora del hardening, fila 4](u3/hardening.md#4-bitacora-de-la-revision)). La clave de la API del USGS nunca se escribe en un archivo: se lee de la variable `USGS_API_KEY`.

## Commits

- Desde la Unidad 2, los mensajes dicen qué agrega el commit y de qué unidad es (`U2 Patricia: ...`, `U3: ...`).
- Cada dimensión se registra con el usuario de git de su integrante ([Equipo](equipo.md#evidencia-en-el-repositorio)).
- Solo se agregan archivos por nombre: nunca `git add .`, para no subir artefactos o credenciales por accidente.

## Criterios de reproducibilidad

| Criterio | Cómo se cumple |
|---|---|
| Todo lo necesario para levantar el sistema está en el repositorio | `kafka/`, `obs/`, `uso-agua-usgs/`, `pyspark/` y `scripts/` |
| Versiones fijadas | Imágenes con versión exacta; dependencias de Python fijadas en el `Dockerfile` y en `requirements.txt` |
| Un solo comando para levantar | `scripts/levantar.ps1`, que espera a que cada servicio esté listo |
| Los artefactos se regeneran, no se copian | Cuadernos `04` y `12b`; el cuaderno `13` dice por nombre qué artefacto falta |
| Rutas relativas al repositorio | Dentro del contenedor, `/opt` es `pyspark/sesiones/` |
| Documentación publicada | Este sitio, generado con MkDocs Material desde `docs/` |

## Publicar esta documentación

```
py -m pip install mkdocs-material
mkdocs serve
mkdocs gh-deploy
```

`mkdocs serve` la muestra en `http://127.0.0.1:8000` para revisarla. `mkdocs gh-deploy` la construye y la publica en la rama `gh-pages` (GitHub Pages).
