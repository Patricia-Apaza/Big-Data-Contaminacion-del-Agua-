# Crea los topicos del sistema si no existen (KAFKA_AUTO_CREATE_TOPICS_ENABLE=false).
# Necesario tras recrear el contenedor de Kafka: no tiene volumen (riesgo aceptado), asi que
# recrearlo vacia los topicos. --if-not-exists no toca un topico que ya existe.
$kt = "/opt/kafka/bin/kafka-topics.sh"
$topicos = @(
  @("agua-usgs-eventos-v2", 3),   # lecturas IoT, key = estacion_id
  @("orden-eventos", 1),          # evento de negocio de Patricia
  @("alerta-eventos", 1)          # evento de negocio de Jhenderson
)
foreach ($t in $topicos) {
  docker exec lambda26-kafka $kt --bootstrap-server localhost:9092 --create --if-not-exists --topic $t[0] --partitions $t[1] --replication-factor 1
}
docker exec lambda26-kafka $kt --bootstrap-server localhost:9092 --describe --topic agua-usgs-eventos-v2
docker exec lambda26-kafka $kt --bootstrap-server localhost:9092 --list
