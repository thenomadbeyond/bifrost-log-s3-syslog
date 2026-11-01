# bifrost-s3-syslog

Liest Bifrost-Logs (JSON) aus einem S3-Bucket – auch über beliebig viele Unterpfade – und
schickt jeden Datensatz als Syslog-Nachricht an rsyslog. Ein State in SQLite sorgt dafür,
dass nichts doppelt gesendet wird.

## Bauen & starten

```bash
docker build -t bifrost-s3-syslog .
docker compose up -d          # Beispielkonfiguration in docker-compose.yml anpassen
```

Einmaliger Lauf (z. B. für Cron / Kubernetes CronJob):

```bash
docker run --rm -v bifrost-state:/data \
  -e S3_BUCKET=my-bifrost-logs -e S3_PREFIXES=logs/ -e AWS_REGION=eu-central-1 \
  -e SYSLOG_HOST=rsyslog.example.local -e RUN_MODE=once \
  bifrost-s3-syslog
```

## Wie Doppelversand verhindert wird

Pro S3-Objekt merkt sich der State `key`, `etag`, `records_sent` und `done`.

| Situation | Verhalten |
|---|---|
| Objekt bereits fertig, ETag gleich | übersprungen |
| Lauf mittendrin abgebrochen | weiter ab letztem Commit |
| Objekt wurde überschrieben (ETag neu) | `ON_OBJECT_CHANGE`: `resume` = nur neue Datensätze am Ende (Default), `resend` = alles neu, `skip` = ignorieren |
| Verbindung zu rsyslog bricht im Batch ab | Batch ab letztem Commit neu senden → nichts geht verloren |

Bei **TCP** wird vor jedem Commit (alle `COMMIT_EVERY` Datensätze und am Objektende) die
Verbindung halb geschlossen und auf das EOF von rsyslog gewartet – erst wenn rsyslog alles
gelesen hat, wird der Fortschritt gespeichert. Doppelte kann es daher nur in dem einen Batch
geben, in dem ein Fehler passiert (max. `COMMIT_EVERY` Nachrichten). Bei **TLS** und **UDP**
ist diese Bestätigung nicht möglich; UDP kann zusätzlich Nachrichten verlieren.

**Wichtig:** Nur **eine** Instanz gleichzeitig laufen lassen (CronJob mit
`concurrencyPolicy: Forbid`, Compose mit einem Replica).

## State persistent halten

- **Volume** (Standard): `/data/state.db` auf einem Volume.
- **S3**: `STATE_S3_URI=s3://bucket/bifrost-shipper/state.db` – State wird beim Start geladen,
  alle `STATE_SYNC_SECONDS` und am Laufende hochgeladen. Praktisch für stateless Container.
  Ein Volume zusätzlich schadet nicht (schnellere Wiederaufnahme nach Crash).

## Konfiguration (Umgebungsvariablen)

| Variable | Default | Beschreibung |
|---|---|---|
| `S3_BUCKET` | – (Pflicht) | Bucket mit den Logs |
| `S3_PREFIXES` | *(ganzer Bucket)* | Pfade, kommagetrennt, z. B. `logs/prod/,logs/stage/` |
| `S3_SUFFIXES` | `.json,.jsonl,.ndjson,.json.gz,.jsonl.gz,.gz` | nur diese Dateiendungen |
| `S3_ENDPOINT_URL` | – | für MinIO/Ceph/Hetzner etc. |
| `S3_FORCE_PATH_STYLE` | `false` | Path-Style-Adressierung |
| `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | – | Standard-AWS-Credentials (oder IAM-Rolle / IRSA) |
| `SYSLOG_HOST` | – (Pflicht) | rsyslog-Server |
| `SYSLOG_PORT` | `514` | |
| `SYSLOG_PROTOCOL` | `tcp` | `tcp`, `tls`, `udp` |
| `SYSLOG_FRAMING` | `octet` | `octet` (RFC 6587) oder `newline` |
| `SYSLOG_FORMAT` | `rfc5424` | `rfc5424` oder `rfc3164` |
| `SYSLOG_FACILITY` / `SYSLOG_SEVERITY` | `local0` / `info` | |
| `SYSLOG_APP_NAME` | `bifrost` | APP-NAME / Tag |
| `SYSLOG_HOSTNAME` | Container-Hostname | |
| `SYSLOG_INCLUDE_S3_SD` | `true` | Bucket + Key als Structured Data (`[s3@32473 …]`) |
| `SYSLOG_TLS_CA_FILE`, `SYSLOG_TLS_VERIFY` | – / `true` | TLS-Einstellungen |
| `SYSLOG_MAX_MSG_BYTES` | `0` | Nachrichten kürzen (0 = aus) |
| `SYSLOG_RETRIES`, `SYSLOG_TIMEOUT` | `5` / `10` | |
| `STATE_DB` | `/data/state.db` | SQLite-State |
| `STATE_S3_URI`, `STATE_SYNC_SECONDS` | – / `60` | State zusätzlich in S3 |
| `COMMIT_EVERY` | `500` | Datensätze pro Commit/Bestätigung |
| `ON_OBJECT_CHANGE` | `resume` | siehe Tabelle oben |
| `RUN_MODE` | `loop` | `loop` (Dauerbetrieb) oder `once` |
| `INTERVAL_SECONDS` | `300` | Pause zwischen Läufen im Loop-Modus |
| `LOG_LEVEL` | `INFO` | |

## Unterstützte Dateiformate

JSON Lines, JSON-Array, einzelnes JSON-Objekt – jeweils optional gzip-komprimiert.
Jeder Datensatz wird einzeilig (kompaktes JSON) gesendet. Der Syslog-Zeitstempel kommt aus
`timestamp`/`created_at`/`time`/`ts`/`@timestamp` des Datensatzes, sonst „jetzt“.

## rsyslog-Seite

LLM-Logs mit Prompts/Antworten sind oft groß – rsyslogs Standardlimit ist 8 KB:

```
global(maxMessageSize="256k")          # ganz oben in rsyslog.conf
module(load="imtcp")
input(type="imtcp" port="514" ruleset="bifrost")

template(name="bifrostJson" type="string" string="%msg%\n")
ruleset(name="bifrost") {
  action(type="omfile" file="/var/log/bifrost/bifrost.log" template="bifrostJson")
}
```

## Kubernetes

Zwei Varianten unter `k8s/` (Kustomize), gemeinsame Teile in `k8s/common`:

| | `k8s/deployment` | `k8s/cronjob` |
|---|---|---|
| Ablauf | Dauerläufer, `RUN_MODE=loop`, alle 5 min | CronJob `*/5`, `RUN_MODE=once` |
| State | PVC (1 Gi, RWO) | S3 (`STATE_S3_URI`), kein PVC |
| Schutz vor Parallelbetrieb | `replicas: 1` + `strategy: Recreate` | `concurrencyPolicy: Forbid` |
| Gut für | einfachstes Setup, Logs laufend sichtbar | kein Speicher nötig, Läufe als Jobs nachvollziehbar |

```bash
# 1. Werte anpassen: k8s/common/configmap.yaml, Image in k8s/<variante>/kustomization.yaml
# 2. Zugangsdaten: IAM-Rolle am ServiceAccount (empfohlen) ODER Secret:
kubectl -n bifrost-logging create secret generic bifrost-s3-syslog-aws \
  --from-literal=AWS_ACCESS_KEY_ID=... --from-literal=AWS_SECRET_ACCESS_KEY=...
# 3. Eine Variante ausrollen - nicht beide gleichzeitig!
kubectl apply -k k8s/cronjob        # oder: kubectl apply -k k8s/deployment

# Testlauf des CronJobs sofort auslösen
kubectl -n bifrost-logging create job --from=cronjob/bifrost-s3-syslog test-run
kubectl -n bifrost-logging logs -f job/test-run
```

Benötigte S3-Rechte: `s3:ListBucket` und `s3:GetObject` auf den Log-Bucket, für den
CronJob zusätzlich `s3:GetObject`/`s3:PutObject` auf den State-Key. Den State-Präfix
nicht unter `S3_PREFIXES` legen.

Wechsel zwischen den Varianten: Bei Deployment → CronJob vorher `state.db` aus dem PVC
nach `STATE_S3_URI` kopieren, sonst wird alles erneut gesendet.

## Helm-Chart (OCI)

Das Chart `charts/bifrost-s3-syslog` (Shipper als Deployment, State in S3 oder PVC) wird von
der CI nach `oci://ghcr.io/thenomadbeyond/charts/bifrost-s3-syslog` gepusht: `main` als
`0.0.0-dev.<run>` (Image-Tag `sha-<7>`), Tag `vX.Y.Z` als `X.Y.Z`.

```bash
helm install bifrost-s3-syslog oci://ghcr.io/thenomadbeyond/charts/bifrost-s3-syslog \
  --version 0.0.0-dev.<run> -n bifrost-logging \
  --set config.S3_BUCKET=my-bifrost-logs --set config.S3_PREFIXES=logs/ \
  --set config.SYSLOG_HOST=rsyslog --set state.s3Uri=s3://my-bifrost-logs/_shipper/state.db \
  --set existingSecret=bifrost-s3-syslog-aws
```

Pflicht: `config.S3_BUCKET`, `config.SYSLOG_HOST`, bei `state.type=s3` (Default) auch
`state.s3Uri`. Alle Shipper-Variablen aus der Tabelle oben gehen unter `config.*`; Credentials
kommen aus einem bestehenden Secret (`existingSecret`) oder per IRSA (`serviceAccount.annotations`).
Dateien: `charts/bifrost-s3-syslog/values.yaml`. Der rsyslog gehoert nicht zum Chart.

## Image bauen (GitHub Actions)

`.github/workflows/build.yml` baut bei jedem Push auf `main` und bei Tags `v*` das Image
und schiebt es nach `ghcr.io/<owner>/bifrost-s3-syslog`. Danach pusht der Job `chart` das Helm-Chart (siehe oben).
