# Levanta el sistema AquaVigia completo, en orden y esperando a que cada parte este LISTA
# (healthcheck + up -d --wait, S14). Uso, desde la raiz del repositorio:
#   powershell -ExecutionPolicy Bypass -File scripts\levantar.ps1
$ErrorActionPreference = "Stop"
function Paso($titulo, [scriptblock]$accion) {
  Write-Host "`n== $titulo" -ForegroundColor Cyan
  & $accion
  if ($LASTEXITCODE -ne 0) { throw "Fallo: $titulo (codigo $LASTEXITCODE)" }
}
Paso "1/5 Kafka (broker, UI, exporter)" { docker compose -f kafka/compose.yml up -d --wait }
Paso "2/5 Topicos"                      { powershell -ExecutionPolicy Bypass -File scripts\crear_topicos.ps1 }
Paso "3/5 PySpark + Jupyter"            { docker compose -f pyspark/compose.yml -f pyspark/compose.kafka.yml up -d --build --wait }
Paso "4/5 Observabilidad"               { docker compose -f obs/compose.yml up -d --wait }
Paso "5/5 MQTT + puente"                { docker compose -f uso-agua-usgs/compose.yml up -d --build --wait }
Write-Host "`n== Estado" -ForegroundColor Cyan
docker ps --filter "name=lambda26" --filter "name=agua-pyspark" --format "{{.Names}}`t{{.Status}}"
