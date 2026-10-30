# Estimación de costos y criterios de escalado (U2, Jhenderson Machaca)

Fecha de consulta de precios: 30-oct-2026. Precios y límites verificados ese día en las páginas oficiales indicadas (Confluent Cloud, plan Basic; AWS EC2 t3, US East Norte de Virginia, Linux bajo demanda). Pueden cambiar después de esa fecha. Los volúmenes salen de lo medido en el cuaderno 12c (sección 7).

## 1. Volumen real medido

| Dato | Valor | Origen |
|---|---|---|
| Series (estación x variable) en el tópico | 66 | 12c, sección 7 |
| Tamaño medio por evento | 322 bytes | 12c, sección 7 |
| Lecturas por serie | 1 cada 5 min = 288 al día | contrato del sensor USGS |
| Régimen estable | 19 008 eventos/día | 66 x 288 |
| Volumen diario | 6,11 MB/día (unos 71 bytes por segundo) | 19 008 x 322 B |
| Volumen mensual | 0,183 GB/mes | 6,11 MB x 30 |
| Capa batch (historial de 90 días) | 1 309 777 lecturas, unos 0,42 GB | descarga del historial x 322 B |

La medición bruta del tópico (40 069 eventos/día) NO se usa: incluye la carga inicial de 6 horas del productor y unas 106 lecturas reenviadas para las pruebas de reinicio y dato tardío.

## 2. Opciones comparadas

El volumen de la tabla de arriba es el de **todo el topico** (66 series, 6 variables), que es lo que se paga. Mis dos variables (pH y turbidez) son 23 de esas series: 23 x 288 = 6 624 eventos al dia, unos 2,1 MB/dia (calculo por multiplicacion).

### A. Kafka administrado: Confluent Cloud, clúster Basic
Fuente: https://confluent.io/confluent-cloud/pricing (consultada el 30-oct-2026)

- Cómputo: la primera eCKU es gratuita, luego USD 0,14 por eCKU-hora.
- Entrada y salida de datos: USD 0,05 por GB.
- Almacenamiento: USD 0,08 por GB-mes.

Cálculo con el volumen real (supuesto: los consumidores leen el doble de lo que se escribe, porque lo leen Spark y el exportador de métricas):

| Concepto | Cuenta | USD/mes |
|---|---|---|
| Cómputo (1 eCKU) | primera eCKU gratuita | 0,00 |
| Entrada | 0,184 GB x 0,05 | 0,009 |
| Salida | 0,184 GB x 2 x 0,05 | 0,018 |
| Almacenamiento del flujo | 0,184 GB x 0,08 | 0,015 |
| Almacenamiento del historial (batch) | 0,42 GB x 0,08 | 0,034 |
| **Total** | | **unos 0,08** |

### B. Kafka propio en una máquina virtual (EC2)
Fuente: https://aws.amazon.com/ec2/instance-types/t3/ (consultada el 30-oct-2026; tabla de US East, Norte de Virginia, Linux)

- t3.medium: USD 0,0418 por hora, o sea unos USD 30,51 al mes si está encendida las 24 horas (0,0418 x 730).
- t3.large: USD 0,0835 por hora, unos USD 60,96 al mes.

No incluye disco (EBS) ni transferencia de datos: esos componentes no se cotizaron y deben sumarse si se elige esta opción.

### Lo que no se usó
La página de Amazon MSK que se consultó es la de las regiones de China (en yuanes), que no corresponde a este proyecto. No se usan sus precios.

## 3. Lectura de la comparación

Con 6 MB al día, el costo del servicio administrado está dominado por el cómputo base, no por los datos: la primera eCKU es gratuita y el resto suma centavos. La máquina propia cuesta unos 30 dólares al mes aunque no mueva casi datos. La diferencia (unos 30 USD frente a menos de 1 USD) se explica casi por completo por pagar una máquina encendida frente a pagar por uso. Hoy el proyecto corre en una laptop compartida del equipo, sin costo de nube.

## 4. Criterios de escalado

El volumen actual es muy pequeño: 1 partición alcanza. Se escala cuando ocurra alguno de estos hechos, que se leen directamente del tablero de Grafana:

1. **La duración del lote se acerca al intervalo del disparador.** Hoy el lote dura 1,46 s (tablero de Grafana) y el disparador es de 20 s. Si pasa de unos 15 s, hay que subir recursos de Spark o repartir las series entre más lotes.
2. **El retraso de publicación supera al watermark.** Hoy el watermark es de 85 minutos y la mediana del retraso es de 28 minutos. Si el retraso se acerca a 85, hay que revisar la fuente o ampliar el watermark (a costa de más estado en memoria).
3. **El atraso del consumidor crece sin parar** (lag en Kafka UI o en el exportador). Es la señal para añadir particiones y consumidores.
4. **Crece el número de estaciones o de variables.** El volumen es lineal: cada serie nueva suma 288 eventos al día, unos 93 KB al día. Con 10 veces más series (660) serían unos 61 MB al día, todavía lejos de un límite práctico de una sola partición.

Límites del plan Basic, según la página oficial de precios de Confluent (consultada el 30-oct-2026): 250 MBps de entrada y 750 MBps de salida, 1 500 particiones y 5 TB de almacenamiento, con SLA de 99,5 %. El volumen actual (unos 71 bytes por segundo) está muy por debajo de esos límites; incluso con 10 veces más series seguiría siendo una fracción mínima de la capacidad.

## 5. Qué decir si preguntan

- "El costo estimado con mis datos reales es de unos 0,08 dólares al mes en un servicio administrado, o unos 30 dólares al mes en una máquina propia, sin contar disco ni transferencia."
- "Esos precios los verifiqué en las páginas oficiales el 30 de octubre de 2026; pueden cambiar después."
- "El volumen es de 19 008 eventos al día. La cifra bruta de 40 069 incluye la carga inicial y lecturas reenviadas para pruebas, por eso no la uso."
