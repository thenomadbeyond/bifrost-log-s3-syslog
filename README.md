# bifrost-s3-syslog

Reads Bifrost logs (JSON) from an S3 bucket - including any number of sub-paths - and sends
every record as a syslog message to rsyslog. A SQLite state ensures nothing is sent twice.

## Build & run

```bash
docker build -t bifrost-s3-syslog .
docker compose up -d          # adjust the example configuration in docker-compose.yml
```

One-off run (e.g. for cron / a Kubernetes CronJob):

```bash
docker run --rm -v bifrost-state:/data \
  -e S3_BUCKET=my-bifrost-logs -e S3_PREFIXES=logs/ -e AWS_REGION=eu-central-1 \
  -e SYSLOG_HOST=rsyslog.example.local -e RUN_MODE=once \
  bifrost-s3-syslog
```

## How duplicate delivery is prevented

For every S3 object the state remembers `key`, `etag`, `records_sent` and `done`.

| Situation | Behaviour |
|---|---|
| Object already done, same ETag | skipped |
| Run aborted midway | resumes from the last commit |
| Object was overwritten (new ETag) | `ON_OBJECT_CHANGE`: `resume` = only new records at the end (default), `resend` = everything again, `skip` = ignore |
| Connection to rsyslog drops within a batch | batch is resent from the last commit -> nothing is lost |

With **TCP**, before every commit (every `COMMIT_EVERY` records and at the end of an object) the
connection is half-closed and the shipper waits for rsyslog's EOF - only once rsyslog has read
everything is the progress saved. Duplicates can therefore only occur in the one batch in which
an error happens (at most `COMMIT_EVERY` messages). With **TLS** and **UDP** this acknowledgement
is not possible; UDP can additionally lose messages.

**Important:** run only **one** instance at a time (CronJob with
`concurrencyPolicy: Forbid`, Compose with a single replica).

## Keeping the state persistent

- **Volume** (default): `/data/state.db` on a volume.
- **S3**: `STATE_S3_URI=s3://bucket/bifrost-shipper/state.db` - the state is loaded at startup and
  uploaded every `STATE_SYNC_SECONDS` and at the end of a run. Handy for stateless containers.
  An additional volume does no harm (faster recovery after a crash).

## Configuration (environment variables)

| Variable | Default | Description |
|---|---|---|
| `S3_BUCKET` | - (required) | Bucket containing the logs |
| `S3_PREFIXES` | *(whole bucket)* | Paths, comma-separated, e.g. `logs/prod/,logs/stage/` |
| `S3_SUFFIXES` | `.json,.jsonl,.ndjson,.json.gz,.jsonl.gz,.gz` | only these file extensions |
| `S3_ENDPOINT_URL` | - | for MinIO/Ceph/Hetzner/Garage etc. |
| `S3_FORCE_PATH_STYLE` | `false` | path-style addressing |
| `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | - | standard AWS credentials (or IAM role / IRSA) |
| `SYSLOG_HOST` | - (required) | rsyslog server |
| `SYSLOG_PORT` | `514` | |
| `SYSLOG_PROTOCOL` | `tcp` | `tcp`, `tls`, `udp` |
| `SYSLOG_FRAMING` | `octet` | `octet` (RFC 6587) or `newline` |
| `SYSLOG_FORMAT` | `rfc5424` | `rfc5424` or `rfc3164` |
| `SYSLOG_FACILITY` / `SYSLOG_SEVERITY` | `local0` / `info` | |
| `SYSLOG_APP_NAME` | `bifrost` | APP-NAME / tag |
| `SYSLOG_HOSTNAME` | container hostname | |
| `SYSLOG_INCLUDE_S3_SD` | `true` | bucket + key as structured data (`[s3@32473 …]`) |
| `SYSLOG_TLS_CA_FILE`, `SYSLOG_TLS_VERIFY` | - / `true` | TLS settings |
| `SYSLOG_MAX_MSG_BYTES` | `0` | truncate messages (0 = off) |
| `SYSLOG_RETRIES`, `SYSLOG_TIMEOUT` | `5` / `10` | |
| `STATE_DB` | `/data/state.db` | SQLite state |
| `STATE_S3_URI`, `STATE_SYNC_SECONDS` | - / `60` | additionally keep the state in S3 |
| `COMMIT_EVERY` | `500` | records per commit/acknowledgement |
| `ON_OBJECT_CHANGE` | `resume` | see table above |
| `RUN_MODE` | `loop` | `loop` (continuous) or `once` |
| `INTERVAL_SECONDS` | `300` | pause between runs in loop mode |
| `LOG_LEVEL` | `INFO` | |

## Supported file formats

JSON Lines, JSON array, single JSON object - each optionally gzip-compressed.
Every record is sent on a single line (compact JSON). The syslog timestamp is taken from
`timestamp`/`created_at`/`time`/`ts`/`@timestamp` of the record, otherwise "now".

## rsyslog side

LLM logs with prompts/responses are often large - rsyslog's default limit is 8 KB. Bifrost
records can reach several hundred KB; rsyslog also rejects large octet-counted TCP frames
even with a high `maxMessageSize`, so use `SYSLOG_FRAMING=newline` for such logs:

```
global(maxMessageSize="4m")            # at the very top of rsyslog.conf
module(load="imtcp")
input(type="imtcp" port="514" ruleset="bifrost")

template(name="bifrostJson" type="string" string="%msg%\n")
ruleset(name="bifrost") {
  action(type="omfile" file="/var/log/bifrost/bifrost.log" template="bifrostJson")
}
```

A minimal rsyslog image (Alpine, non-root, port 5514) is in `rsyslog/Dockerfile`.

## Kubernetes

Two variants under `k8s/` (Kustomize), shared parts in `k8s/common`:

| | `k8s/deployment` | `k8s/cronjob` |
|---|---|---|
| Operation | long-running, `RUN_MODE=loop`, every 5 min | CronJob `*/5`, `RUN_MODE=once` |
| State | PVC (1 Gi, RWO) | S3 (`STATE_S3_URI`), no PVC |
| Protection against parallel runs | `replicas: 1` + `strategy: Recreate` | `concurrencyPolicy: Forbid` |
| Good for | simplest setup, logs visible continuously | no storage needed, runs traceable as jobs |

```bash
# 1. Adjust values: k8s/common/configmap.yaml, image in k8s/<variant>/kustomization.yaml
# 2. Credentials: IAM role on the ServiceAccount (recommended) OR a Secret:
kubectl -n bifrost-logging create secret generic bifrost-s3-syslog-aws \
  --from-literal=AWS_ACCESS_KEY_ID=... --from-literal=AWS_SECRET_ACCESS_KEY=...
# 3. Roll out one variant - not both at once!
kubectl apply -k k8s/cronjob        # or: kubectl apply -k k8s/deployment

# Trigger a test run of the CronJob right away
kubectl -n bifrost-logging create job --from=cronjob/bifrost-s3-syslog test-run
kubectl -n bifrost-logging logs -f job/test-run
```

Required S3 permissions: `s3:ListBucket` and `s3:GetObject` on the log bucket; for the
CronJob additionally `s3:GetObject`/`s3:PutObject` on the state key. Do not put the state
prefix under `S3_PREFIXES`.

Switching between the variants: when going from Deployment to CronJob, first copy `state.db`
from the PVC to `STATE_S3_URI`, otherwise everything is sent again.

## Helm chart (OCI)

The chart `charts/bifrost-s3-syslog` (shipper as a Deployment, state in S3 or on a PVC) is
pushed by CI to `oci://ghcr.io/thenomadbeyond/charts/bifrost-s3-syslog`: `main` as
`0.0.0-dev.<run>` (image tag `sha-<7>`), tag `vX.Y.Z` as `X.Y.Z`.

```bash
helm install bifrost-s3-syslog oci://ghcr.io/thenomadbeyond/charts/bifrost-s3-syslog \
  --version 0.0.0-dev.<run> -n bifrost-logging \
  --set config.S3_BUCKET=my-bifrost-logs --set config.S3_PREFIXES=logs/ \
  --set config.SYSLOG_HOST=rsyslog --set state.s3Uri=s3://my-bifrost-logs/_shipper/state.db \
  --set existingSecret=bifrost-s3-syslog-aws
```

Required: `config.S3_BUCKET`, `config.SYSLOG_HOST`, and with `state.type=s3` (default) also
`state.s3Uri`. All shipper variables from the table above go under `config.*`; credentials
come from an existing Secret (`existingSecret`) or via IRSA (`serviceAccount.annotations`).
See `charts/bifrost-s3-syslog/values.yaml`. rsyslog is not part of the chart.

## Building the image (GitHub Actions)

`.github/workflows/build.yml` builds the image on every push to `main` and on tags `v*`
and pushes it to `ghcr.io/<owner>/bifrost-s3-syslog`. Afterwards the `chart` job pushes the
Helm chart (see above).
