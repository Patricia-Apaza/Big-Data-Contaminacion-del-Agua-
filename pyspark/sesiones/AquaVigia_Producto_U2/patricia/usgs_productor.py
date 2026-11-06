#!/usr/bin/env python3
"""
Productor USGS -> Kafka / archivo (AquaVigia U2).

Usa la API nueva de USGS (api.waterdata.usgs.gov/ogcapi/v0), la misma de tu panel HTML.

Comandos:
  descubrir  mide la cadencia real (min entre lecturas) por estacion.
  publicar   consulta CADA estacion en la coleccion 'continuous' (TODAS las lecturas
             recientes, no solo la ultima) y emite solo las que aun no envio.
  historial  baja N dias de historial (para entrenar el modelo y para reproducir).
  reproducir lee un .jsonl y lo manda a Kafka respetando el tiempo del evento.

Por que 'continuous' y no 'latest-continuous': los sensores miden cada 5 min pero
USGS los publica en lotes. 'latest' solo da el ultimo valor y se pierden los
intermedios; 'continuous' los trae todos.

Clave API (opcional, gratis): variable de entorno USGS_API_KEY. NUNCA la guardes en git.
Broker de Kafka desde tu PC: localhost:49092 (el 9092 solo existe dentro de Docker).

Contrato del evento, version 2 (version_esquema = 2):
  Igual que la version 1 y ademas el campo "contexto", que lleva lo que el modelo
  necesita para pronosticar con UNA sola lectura (inferencia sin estado en Spark):
    contexto.rezagos     valores de la MISMA variable 5, 10, 15, 30 y 60 min antes (l1, l2, l3, l6, l12)
    contexto.simultaneas valores de las OTRAS variables de la estacion en el mismo bloque de 5 min
  Los consumidores de la version 1 siguen funcionando: solo ignoran el campo nuevo.

Salidas: stdout, kafka (directo, para pruebas) o mqtt (como un dispositivo real:
publica en MQTT y el puente bridge_mqtt_kafka_usgs.py lo pasa a Kafka).
"""
import argparse, json, os, statistics, sys, time, uuid, urllib.parse, urllib.request, urllib.error
from datetime import datetime, timezone, timedelta

BASE = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/"
VARS = {"00400": "ph", "00300": "oxigeno_mgl", "63680": "turbidez_fnu",
        "00095": "conductividad_uscm", "00010": "temperatura_c", "99133": "nitrato_mgl"}
CODES = ",".join(VARS)
VERSION_ESQUEMA = 2
REZAGOS = {"l1": 1, "l2": 2, "l3": 3, "l6": 6, "l12": 12}   # en bloques de 5 min


def jget(url, tolerar_400=False):
    h = {"Accept": "application/json"}
    if os.environ.get("USGS_API_KEY"):
        h["X-Api-Key"] = os.environ["USGS_API_KEY"]
    for intento in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=90) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 400 and tolerar_400:
                return None
            if e.code == 429:
                espera = 30 * (intento + 1)
                print(f"USGS 429 (demasiadas consultas). Espero {espera}s...", file=sys.stderr, flush=True)
                time.sleep(espera)
                continue
            sys.exit(f"USGS respondio {e.code}: {e.read()[:150]!r}")
        except (urllib.error.URLError, TimeoutError) as e:
            print(f"Red/timeout ({e}). Reintento...", file=sys.stderr, flush=True)
            time.sleep(10)
    sys.exit("USGS no respondio tras 4 intentos.")


def props(d):
    return [f.get("properties", {}) for f in (d or {}).get("features", [])]


def parse_t(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def estaciones_activas(estado, codigo="00400", dias=3):
    q = (BASE + "time-series-metadata/items?f=json&skipGeometry=TRUE&limit=50000"
         + ("&state_name=" + urllib.parse.quote(estado) if estado else "")
         + "&parameter_code=" + codigo + "&properties=monitoring_location_id,parameter_code,end_utc")
    corte = time.time() - dias * 86400
    ids = set()
    for p in props(jget(q)):
        e = p.get("end_utc")
        if e and parse_t(e).timestamp() < corte:
            continue
        if p.get("monitoring_location_id"):
            ids.add(p["monitoring_location_id"])
    return sorted(ids)


def cadencia_min(tiempos):
    ts = sorted(t.timestamp() for t in tiempos)
    d = [(b - a) / 60 for a, b in zip(ts, ts[1:]) if b > a]
    return statistics.median(d) if len(d) >= 2 else None


def serie(est_id, codigos, periodo, limite=10000):
    """Todas las lecturas de la estacion en el periodo (ej. PT6H, P1D, P30D)."""
    q = (BASE + "continuous/items?f=json&skipGeometry=TRUE&limit=" + str(limite)
         + "&monitoring_location_id=" + est_id + "&parameter_code=" + codigos
         + "&time=" + periodo + "&properties=parameter_code,value,time")
    d = jget(q, tolerar_400=True)
    return props(d) if d is not None else None


def serie_rango(est_id, codigo, ini, fin, limite=10000):
    """Lecturas entre dos instantes (datetime UTC), formato de intervalo OGC 'ini/fin'."""
    f = lambda d: d.strftime("%Y-%m-%dT%H:%M:%SZ")
    q = (BASE + "continuous/items?f=json&skipGeometry=TRUE&limit=" + str(limite)
         + "&monitoring_location_id=" + est_id + "&parameter_code=" + codigo
         + "&time=" + f(ini) + "/" + f(fin) + "&properties=parameter_code,value,time")
    d = jget(q, tolerar_400=True)
    return props(d) if d is not None else None


def historial_tiempos(est_id, codigo="00400", periodo="P1D"):
    f = serie(est_id, codigo, periodo) or []
    return [parse_t(p["time"]) for p in f if p.get("time")]


def descubrir(a):
    estados = [e.strip() for e in a.estado.split(",") if e.strip()] or [""]
    if a.estado.strip().lower() == "todos":
        estados = ["Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","District of Columbia","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming","Puerto Rico"]
    filas = []
    try:
        for est in estados:
            ids = estaciones_activas(est)
            print(f"\n[{est or 'EE. UU.'}] {len(ids)} estaciones con pH activas. Midiendo cadencia (max {a.max})...", flush=True)
            for i in ids[: a.max]:
                c = cadencia_min(historial_tiempos(i))
                filas.append((est, i, c))
                print(f"  {i:<28} {'sin datos' if c is None else f'{c:.1f} min'}", flush=True)
                time.sleep(a.pausa)
    except SystemExit as e:
        print(f"\nSe detuvo antes de terminar: {e}. Se guarda lo medido hasta aqui.", flush=True)
    ok = [f for f in filas if f[2] is not None and f[2] <= a.limite]
    with open("resultados_cadencia.csv", "w", encoding="utf-8") as fh:
        fh.write("estado,estacion_id,cadencia_min\n")
        for e, i, c in filas:
            fh.write(f"{e},{i},{'' if c is None else round(c,1)}\n")
    print(f"\nEstaciones con lectura cada {a.limite:g} min o menos: {len(ok)} de {len(filas)} medidas")
    for e, i, c in sorted(ok, key=lambda x: x[2]):
        print(f"  {i}  ({c:.1f} min)  {e}")
    print("\nResultado completo guardado en resultados_cadencia.csv (en la carpeta actual).")


def bloque5(t):
    """Instante -> inicio de su bloque de 5 minutos (epoch en segundos)."""
    return int(t.timestamp()) // 300 * 300


def contexto(indice, est, var, t):
    """indice: {(estacion, variable, bloque5): valor}. Arma el contexto de UNA lectura."""
    b = bloque5(t)
    rez = {k: indice[(est, var, b - n * 300)] for k, n in REZAGOS.items() if (est, var, b - n * 300) in indice}
    sim = {v: indice[(est, v, b)] for v in VARS.values() if v != var and (est, v, b) in indice}
    return {"rezagos": rez, "simultaneas": sim}


def evento(est, cod, valor, t_str, origen, ahora=None):
    ahora = ahora or datetime.now(timezone.utc)
    t = parse_t(t_str)
    return {
        "version_esquema": VERSION_ESQUEMA,
        "evento_id": str(uuid.uuid4()),
        "estacion_id": est,
        "variable": VARS[cod],
        "codigo_parametro": cod,
        "valor": valor,
        "fecha_medicion_utc": t.isoformat(),
        "fecha_ingesta_utc": ahora.isoformat(),
        "retraso_min": round((ahora - t).total_seconds() / 60, 1),
        "origen": origen,
        "fuente": "USGS",
    }


def eventos_nuevos(filas, est, vistos, origen="vivo", ahora=None, con_contexto=False):
    """filas = propiedades devueltas por 'continuous' para UNA estacion.
    Emite cada (estacion, variable, tiempo) una sola vez, ordenado por tiempo.
    con_contexto=True agrega el campo 'contexto' (version 2) usando las mismas filas."""
    out = []
    indice = {}
    if con_contexto:
        for p in filas:
            try:
                indice[(est, VARS[p["parameter_code"]], bloque5(parse_t(p["time"])))] = float(p["value"])
            except (KeyError, TypeError, ValueError):
                pass
    for p in filas:
        cod = p.get("parameter_code")
        if cod not in VARS or not p.get("time"):
            continue
        try:
            v = float(p["value"])
        except (KeyError, TypeError, ValueError):
            continue
        k = (est, cod, parse_t(p["time"]).isoformat())
        if k in vistos:
            continue
        vistos.add(k)
        e = evento(est, cod, v, p["time"], origen, ahora)
        if con_contexto:
            e["contexto"] = contexto(indice, est, VARS[cod], parse_t(p["time"]))
        out.append(e)
    out.sort(key=lambda e: e["fecha_medicion_utc"])
    return out


def ids_desde_args(a):
    ids = [x.strip() for x in (getattr(a, "estaciones", "") or "").split(",") if x.strip()]
    if getattr(a, "desde_csv", ""):
        import csv
        with open(a.desde_csv, encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                c = r.get("cadencia_min")
                if c and float(c) <= a.limite and r["estacion_id"] not in ids:
                    ids.append(r["estacion_id"])
    if not ids:
        sys.exit("No hay estaciones: usa --estaciones o --desde-csv.")
    return ids


def kafka_prod(broker):
    from kafka import KafkaProducer  # pip install kafka-python
    return KafkaProducer(bootstrap_servers=broker,
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                         key_serializer=lambda k: k.encode("utf-8"))


class Salida:
    """Un solo punto de envio: stdout, kafka (directo) o mqtt (hacia el puente)."""
    def __init__(self, a):
        self.tipo, self.topic = a.salida, a.topic
        self.prod = self.cli = None
        if self.tipo == "kafka":
            self.prod = kafka_prod(a.broker)
        elif self.tipo == "mqtt":
            import paho.mqtt.client as mqtt  # pip install paho-mqtt
            self.base = a.mqtt_topic.rstrip("/")
            self.cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"usgs-productor-{uuid.uuid4().hex[:8]}")
            self.cli.connect(a.mqtt_host, a.mqtt_port, keepalive=60)
            self.cli.loop_start()
            print(f"MQTT conectado a {a.mqtt_host}:{a.mqtt_port}, topico {self.base}/<estacion_id>", file=sys.stderr, flush=True)

    def enviar(self, e):
        if self.prod:
            self.prod.send(self.topic, key=e["estacion_id"], value=e)
        elif self.cli:
            # Un topico MQTT por dispositivo (estacion); el puente lo usa como key de Kafka.
            self.cli.publish(f"{self.base}/{e['estacion_id']}", json.dumps(e, ensure_ascii=False), qos=1).wait_for_publish(10)
        else:
            print(json.dumps(e, ensure_ascii=False), flush=True)

    def vaciar(self):
        if self.prod:
            self.prod.flush()

    def cerrar(self):
        self.vaciar()
        if self.cli:
            self.cli.loop_stop(); self.cli.disconnect()


def opciones_salida(p, por_defecto="stdout"):
    p.add_argument("--salida", choices=["stdout", "kafka", "mqtt"], default=por_defecto)
    p.add_argument("--broker", default="localhost:49092")
    p.add_argument("--topic", default="agua-usgs-eventos-v2")
    p.add_argument("--mqtt-host", default="localhost", dest="mqtt_host")
    p.add_argument("--mqtt-port", type=int, default=41883, dest="mqtt_port")
    p.add_argument("--mqtt-topic", default="aquavigia/equipo02/usgs", dest="mqtt_topic")


def publicar(a):
    ids = ids_desde_args(a)
    sal = Salida(a)
    vistos, total = set(), 0
    periodo = a.periodo
    print(f"{len(ids)} estaciones; consulta cada {a.cada}s con ventana {periodo}. Ctrl+C para detener.", file=sys.stderr, flush=True)
    try:
        while True:
            nuevos = 0
            for est in ids:
                filas = serie(est, CODES, periodo)
                if filas is None and periodo != "P1D":
                    print(f"Formato '{periodo}' no aceptado; uso P1D.", file=sys.stderr, flush=True)
                    periodo = "P1D"
                    filas = serie(est, CODES, periodo) or []
                for e in eventos_nuevos(filas or [], est, vistos, "vivo", con_contexto=True):
                    sal.enviar(e)
                    nuevos += 1
                time.sleep(0.3)
            sal.vaciar()
            total += nuevos
            print(f"[{datetime.now():%H:%M:%S}] nuevos: {nuevos}  total: {total}", file=sys.stderr, flush=True)
            time.sleep(a.cada)
    except KeyboardInterrupt:
        print(f"\nDetenido. Eventos emitidos: {total}", file=sys.stderr)
    finally:
        sal.cerrar()


def historial(a):
    ids = ids_desde_args(a)
    wanted = {v.strip() for v in a.variables.split(",") if v.strip()}
    cods = [c for c, n in VARS.items() if not wanted or n in wanted]
    fin = datetime.now(timezone.utc)
    ini_total = fin - timedelta(days=a.dias)
    paso = timedelta(days=a.tramo)
    vistos, total = set(), 0
    with open(a.salida_archivo, "a", encoding="utf-8") as fh:
        for est in ids:
            for cod in cods:
                n_est = 0
                t0 = ini_total
                while t0 < fin:
                    t1 = min(t0 + paso, fin)
                    if a.dias <= 30:
                        filas = serie(est, cod, f"P{a.dias}D")
                        t0 = fin  # una sola consulta
                    else:
                        filas = serie_rango(est, cod, t0, t1)
                        if filas is None:
                            sys.exit("USGS no acepto el rango 'inicio/fin'. Usa --dias 30 y avisame.")
                        t0 = t1
                    filas = filas or []
                    if len(filas) >= 10000:
                        print(f"  AVISO: {est} {VARS[cod]} llego al tope de 10000 en un tramo; baja --tramo.", flush=True)
                    evs = eventos_nuevos(filas, est, vistos, "historial")
                    for e in evs:
                        fh.write(json.dumps(e, ensure_ascii=False) + "\n")
                    n_est += len(evs)
                    time.sleep(0.4)
                total += n_est
                print(f"{est} {VARS[cod]:<20} {n_est:>7} lecturas", flush=True)
    print(f"\nListo: {total} lecturas en {a.salida_archivo}")


def reproducir(a):
    """Reenvia un historial respetando el orden del tiempo del evento.
    --desde/--hasta eligen un episodio concreto (tiempo de medicion UTC).
    --ultimas-horas se cuenta desde la lectura MAS RECIENTE del archivo (no desde ahora),
    para que funcione con un historial descargado dias atras."""
    def fecha(l):
        i = l.find('"fecha_medicion_utc": "')
        return l[i + 23:i + 48] if i >= 0 else ""
    # Primera pasada (rapida, sin parsear JSON): la lectura mas reciente del archivo.
    with open(a.archivo, encoding="utf-8") as fh:
        maxima = max((fecha(l) for l in fh if l.startswith("{")), default="")
    if not maxima:
        sys.exit("El archivo no tiene eventos.")
    t_max = parse_t(maxima)
    corte = t_max - timedelta(hours=a.ultimas_horas) if a.ultimas_horas else None
    fin = None
    if a.desde:
        corte = parse_t(a.desde if "+" in a.desde or a.desde.endswith("Z") else a.desde + "+00:00")
    if a.hasta:
        fin = parse_t(a.hasta if "+" in a.hasta or a.hasta.endswith("Z") else a.hasta + "+00:00")
    # El indice incluye 1 hora antes del corte para que las primeras lecturas tengan sus rezagos.
    margen = corte - timedelta(hours=1) if corte else None
    m_txt = margen.isoformat() if margen else ""
    evs = []
    with open(a.archivo, encoding="utf-8") as fh:
        for l in fh:
            if l.startswith("{") and fecha(l) >= m_txt and (fin is None or fecha(l) < fin.isoformat()):
                evs.append(json.loads(l))
    indice = {}
    for e in evs:
        t = parse_t(e["fecha_medicion_utc"])
        if margen is None or t >= margen:
            indice[(e["estacion_id"], e["variable"], bloque5(t))] = e["valor"]
    if corte:
        evs = [e for e in evs if parse_t(e["fecha_medicion_utc"]) >= corte]
    evs.sort(key=lambda e: e["fecha_medicion_utc"])
    sal = Salida(a)
    print(f"{len(evs)} eventos a reproducir a {a.velocidad:g} eventos/s por {a.salida} "
          f"(desde {evs[0]['fecha_medicion_utc']} hasta {evs[-1]['fecha_medicion_utc']}).", file=sys.stderr, flush=True)
    try:
        for i, e in enumerate(evs, 1):
            ahora = datetime.now(timezone.utc)
            t = parse_t(e["fecha_medicion_utc"])
            e = dict(e, version_esquema=VERSION_ESQUEMA, evento_id=str(uuid.uuid4()), fecha_ingesta_utc=ahora.isoformat(),
                     origen="replay", retraso_min=round((ahora - t).total_seconds() / 60, 1),
                     contexto=contexto(indice, e["estacion_id"], e["variable"], t))
            sal.enviar(e)
            if i % 50 == 0:
                sal.vaciar()
                print(f"  enviados {i}/{len(evs)}", file=sys.stderr, flush=True)
            time.sleep(1.0 / a.velocidad)
    except KeyboardInterrupt:
        print("\nDetenido por el usuario.", file=sys.stderr)
    finally:
        sal.cerrar()
    print("Reproduccion terminada.", file=sys.stderr)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sp = ap.add_subparsers(dest="cmd", required=True)
    d = sp.add_parser("descubrir")
    d.add_argument("--estado", default="New Jersey", help='un estado, varios separados por coma, o "todos"')
    d.add_argument("--max", type=int, default=40)
    d.add_argument("--limite", type=float, default=5.0)
    d.add_argument("--pausa", type=float, default=1.5)
    d.set_defaults(f=descubrir)

    def comun(p):
        p.add_argument("--estaciones", default="")
        p.add_argument("--desde-csv", default="", dest="desde_csv")
        p.add_argument("--limite", type=float, default=5.0)

    p = sp.add_parser("publicar"); comun(p)
    p.add_argument("--cada", type=int, default=120, help="segundos entre rondas")
    p.add_argument("--periodo", default="PT6H", help="ventana que se pide en cada ronda (PT6H, P1D)")
    opciones_salida(p)
    p.set_defaults(f=publicar)
    h = sp.add_parser("historial"); comun(h)
    h.add_argument("--dias", type=int, default=30)
    h.add_argument("--tramo", type=int, default=25, help="dias por consulta cuando --dias > 30")
    h.add_argument("--variables", default="", help="ej: temperatura_c,oxigeno_mgl,ph,turbidez_fnu (vacio = las 6)")
    h.add_argument("--salida-archivo", default="historial.jsonl", dest="salida_archivo")
    h.set_defaults(f=historial)
    r = sp.add_parser("reproducir")
    r.add_argument("--archivo", required=True)
    r.add_argument("--velocidad", type=float, default=200.0, help="eventos por segundo")
    r.add_argument("--ultimas-horas", type=float, default=0, dest="ultimas_horas", help="solo reproduce las ultimas N horas del archivo (0 = todo)")
    r.add_argument("--desde", default="", help="inicio del episodio, tiempo de medicion UTC (ej. 2026-10-16T22:00)")
    r.add_argument("--hasta", default="", help="fin del episodio, sin incluir (ej. 2026-10-17T04:00)")
    opciones_salida(r, por_defecto="kafka")
    r.set_defaults(f=reproducir)
    a = ap.parse_args()
    a.f(a)
