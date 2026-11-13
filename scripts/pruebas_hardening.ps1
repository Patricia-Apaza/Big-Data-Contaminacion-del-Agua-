# Pruebas del hardening (S14) DESPUES de aplicarlo con scripts\levantar.ps1, y estado final.
# Cierra las filas "Pendiente" de la bitacora en docs/u3/hardening.md.
# Uso, desde la raiz del repositorio:   powershell -ExecutionPolicy Bypass -File scripts\pruebas_hardening.ps1
# Tarda unos 2 minutos: detiene Kafka a proposito para probar los reintentos del puente.
$ev = "docs\u3\evidencias"
New-Item -ItemType Directory -Force -Path $ev | Out-Null
function Sello { Get-Date -Format "yyyy-MM-dd HH:mm:ss" }
# cmd /c evita que Windows PowerShell convierta la salida de error de docker en errores de PowerShell
function D($args_) { cmd /c "docker $args_ 2>&1" }
function Salud($c) { (D "inspect -f {{.State.Health.Status}} $c") -join "" }

# 1. Estado de los contenedores (debe decir "healthy")
Write-Host "== 1/4 Estado de los contenedores" -ForegroundColor Cyan
$f = "$ev\docker_ps_despues.txt"
"docker ps DESPUES - $(Sello)" | Set-Content $f
D 'ps --format {{.Names}}\t{{.Status}}' | Add-Content $f
Get-Content $f

# 2. Topicos tras recrear Kafka
Write-Host "`n== 2/4 Topicos" -ForegroundColor Cyan
$f = "$ev\topicos_despues.txt"
"Topicos DESPUES - $(Sello)" | Set-Content $f
D "exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe" | Add-Content $f
D "exec lambda26-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092" | Add-Content $f
Get-Content $f

# 3. Credenciales de Grafana por variable de entorno (bitacora, fila 2)
Write-Host "`n== 3/4 Variable de entorno de Grafana" -ForegroundColor Cyan
$f = "$ev\grafana_variable_prueba.txt"
"Prueba de la variable GRAFANA_ADMIN_PASSWORD - $(Sello)" | Set-Content $f
"-- Sin variable (valor por defecto):" | Add-Content $f
Remove-Item Env:GRAFANA_ADMIN_PASSWORD -ErrorAction SilentlyContinue
D "compose -f obs/compose.yml config" | Select-String "GF_SECURITY_ADMIN" | ForEach-Object { $_.Line.Trim() } | Add-Content $f
"-- Con `$env:GRAFANA_ADMIN_PASSWORD='prueba-variable-s14':" | Add-Content $f
$env:GRAFANA_ADMIN_PASSWORD = "prueba-variable-s14"
D "compose -f obs/compose.yml config" | Select-String "GF_SECURITY_ADMIN" | ForEach-Object { $_.Line.Trim() } | Add-Content $f
Remove-Item Env:GRAFANA_ADMIN_PASSWORD
"(La prueba solo usa 'config': no recrea Grafana ni cambia su clave real.)" | Add-Content $f
Get-Content $f

# 4. Reintentos del puente con Kafka detenido (bitacora, fila 3)
Write-Host "`n== 4/4 Puente con Kafka detenido" -ForegroundColor Cyan
$f = "$ev\puente_reintento_prueba.txt"
"Prueba de reintentos del puente - $(Sello)" | Set-Content $f
$t0 = Get-Date
"-- $(Sello) docker stop lambda26-kafka" | Add-Content $f;   D "stop lambda26-kafka" | Out-Null
"-- $(Sello) docker restart lambda26-puente-usgs" | Add-Content $f;   D "restart lambda26-puente-usgs" | Out-Null
Start-Sleep 25
"-- $(Sello) estado del puente con Kafka detenido: $(Salud 'lambda26-puente-usgs')" | Add-Content $f
"-- $(Sello) docker start lambda26-kafka" | Add-Content $f;   D "start lambda26-kafka" | Out-Null
$limite = (Get-Date).AddSeconds(180)
while ((Salud "lambda26-kafka") -ne "healthy" -and (Get-Date) -lt $limite) { Start-Sleep 5 }
"-- $(Sello) Kafka: $(Salud 'lambda26-kafka')" | Add-Content $f
Start-Sleep 45
$seg = [int]((Get-Date) - $t0).TotalSeconds + 5
"-- Log del puente desde el reinicio:" | Add-Content $f
D "logs lambda26-puente-usgs --since ${seg}s" | Add-Content $f
"-- $(Sello) estado final del puente: $(Salud 'lambda26-puente-usgs')" | Add-Content $f
"-- Topicos tras detener y arrancar Kafka (el contenedor no se recrea, los datos siguen):" | Add-Content $f
D "exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list" | Add-Content $f
Get-Content $f
Write-Host "`nGuardado en $ev : docker_ps_despues.txt, topicos_despues.txt, grafana_variable_prueba.txt, puente_reintento_prueba.txt"
