# Prueba A MANO cada comando de healthcheck dentro de los contenedores que YA estan corriendo
# (S14, 3.3: antes de escribir un healthcheck, se prueba; un comando que siempre falla dejaria
# el servicio "unhealthy" para siempre). Tambien guarda el punto de partida (ANTES del cambio).
# Uso, desde la raiz del repositorio:   powershell -ExecutionPolicy Bypass -File scripts\probar_salud.ps1
$ev = "docs\u3\evidencias"
New-Item -ItemType Directory -Force -Path $ev | Out-Null
$sello = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

$pruebas = @(
  @("lambda26-kafka",          @("/opt/kafka/bin/kafka-broker-api-versions.sh","--bootstrap-server","localhost:9092")),
  @("lambda26-kafka-exporter", @("wget","-q","--spider","http://localhost:9308/metrics")),
  @("lambda26-prometheus",     @("wget","-q","--spider","http://localhost:9090/-/ready")),
  @("lambda26-pushgateway",    @("wget","-q","--spider","http://localhost:9091/-/ready")),
  @("lambda26-grafana",        @("wget","-q","--spider","http://localhost:3000/api/health")),
  @("lambda26-mosquitto",      @("mosquitto_sub","-h","localhost","-p","1883","-t",'$SYS/broker/uptime',"-C","1","-W","5")),
  @("lambda26-puente-usgs",    @("python","-c","import socket; socket.create_connection(('mosquitto',1883),3); socket.create_connection(('kafka',9092),3)")),
  @("agua-pyspark",            @("python","-c","import urllib.request; urllib.request.urlopen('http://localhost:8888/api', timeout=5)"))
)
$salida = @("Prueba manual de healthchecks - $sello")
foreach ($p in $pruebas) {
  $c = $p[0]; $cmd = $p[1]
  docker exec $c @cmd *> $null
  $code = $LASTEXITCODE
  $linea = "{0,-26} codigo={1}  {2}" -f $c, $code, $(if ($code -eq 0) {"OK"} else {"FALLA"})
  Write-Host $linea
  $salida += $linea
}
$salida | Set-Content "$ev\healthchecks_prueba_manual.txt"

# Punto de partida ANTES de aplicar el hardening
"docker ps ANTES - $sello" | Set-Content "$ev\docker_ps_antes.txt"
docker ps --format "{{.Names}}`t{{.Status}}" | Add-Content "$ev\docker_ps_antes.txt"
"Topicos ANTES - $sello" | Set-Content "$ev\topicos_antes.txt"
docker exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe | Add-Content "$ev\topicos_antes.txt"
docker exec lambda26-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 | Add-Content "$ev\topicos_antes.txt"
Write-Host "`nGuardado en $ev : healthchecks_prueba_manual.txt, docker_ps_antes.txt, topicos_antes.txt"
