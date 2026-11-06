"""
Puente MQTT -> Kafka para las lecturas USGS (AquaVigia U2, Equipo 02).

Por que existe: un dispositivo de campo (sensor, datalogger, ESP32) no habla Kafka.
El protocolo de Kafka es pesado (TCP persistente, metadatos del cluster, lotes,
reintentos) y exige exponer el broker a la red de campo. El dispositivo publica por
MQTT, que es liviano, y este puente, que corre junto al cluster, lo pasa a Kafka.

Reglas (las mismas del puente de S7):
  - No valida ni transforma el payload: lo reenvia tal cual. Esquema y rango fisico
    se validan en un solo lugar, en Spark (cuadernos 12a/12d y 12c).
  - Topico MQTT por dispositivo: aquavigia/equipo02/usgs/<estacion_id>.
    El ultimo segmento se usa como KEY de Kafka, asi todas las lecturas de una
    estacion caen en la misma particion y conservan su orden.
"""
import json
import os
import time
import uuid

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "aquavigia/equipo02/usgs/#")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "agua-usgs-eventos-v2")
LOG_CADA = int(os.getenv("LOG_CADA", "100"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: v,
    acks="all",
    linger_ms=20,
)
cuenta = {"reenviados": 0, "por_particion": {}}


def log(**k):
    print(json.dumps({"service": "puente-usgs", **k}, ensure_ascii=False), flush=True)


def al_confirmar(meta):
    p = str(meta.partition)
    cuenta["por_particion"][p] = cuenta["por_particion"].get(p, 0) + 1


def on_connect(client, userdata, flags, reason_code, properties=None):
    log(status="connected" if reason_code == 0 else f"connect_failed:{reason_code}", mqttHost=MQTT_HOST, mqttTopic=MQTT_TOPIC)
    client.subscribe(MQTT_TOPIC, qos=1)


def on_message(client, userdata, msg):
    estacion = msg.topic.rsplit("/", 1)[-1] or "desconocido"
    # Cabeceras Kafka: dejan rastro de por donde entro el evento sin tocar el payload.
    fut = producer.send(KAFKA_TOPIC, key=estacion, value=msg.payload,
                        headers=[("canal", b"mqtt"), ("mqtt_topic", msg.topic.encode("utf-8"))])
    fut.add_callback(al_confirmar)
    cuenta["reenviados"] += 1
    if cuenta["reenviados"] % LOG_CADA == 1:
        meta = fut.get(timeout=10)
        log(status="forwarded", mqttTopic=msg.topic, kafkaTopic=meta.topic, key=estacion,
            partition=meta.partition, offset=meta.offset, total=cuenta["reenviados"],
            porParticion=cuenta["por_particion"])


log(status="starting", mqttHost=MQTT_HOST, mqttPort=MQTT_PORT, kafka=KAFKA_BOOTSTRAP_SERVERS, kafkaTopic=KAFKA_TOPIC)
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"puente-usgs-{uuid.uuid4().hex[:8]}")
client.on_connect = on_connect
client.on_message = on_message
while True:
    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        break
    except OSError as e:
        log(status="mqtt_no_disponible", error=str(e)); time.sleep(3)
client.loop_forever()
