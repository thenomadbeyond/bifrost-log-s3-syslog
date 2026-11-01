#!/usr/bin/env python3
"""
bifrost-s3-syslog
Liest Bifrost-JSON-Logs aus S3 und sendet jeden Datensatz als Syslog-Nachricht
an einen rsyslog-Server. Ein SQLite-State verhindert Doppelversand.

State pro S3-Objekt: (bucket, key, etag, records_sent, done)
- Fortschritt wird alle COMMIT_EVERY Datensätze gespeichert -> nach Abbruch
  wird ab dem letzten Commit weitergemacht.
- Bei TCP wird vor jedem Commit die Verbindung sauber geschlossen und auf das
  EOF von rsyslog gewartet. Erst dann gilt der Batch als zugestellt.
"""
import datetime
import gzip
import json
import logging
import os
import re
import signal
import socket
import sqlite3
import ssl
import sys
import tempfile
import time

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

log = logging.getLogger("bifrost-s3-syslog")
UTC = datetime.timezone.utc

FACILITIES = {
    "kern": 0, "user": 1, "mail": 2, "daemon": 3, "auth": 4, "syslog": 5,
    "lpr": 6, "news": 7, "uucp": 8, "cron": 9, "authpriv": 10, "ftp": 11,
    **{f"local{i}": 16 + i for i in range(8)},
}
SEVERITIES = {
    "emerg": 0, "alert": 1, "crit": 2, "err": 3,
    "warning": 4, "notice": 5, "info": 6, "debug": 7,
}
TS_KEYS = ("timestamp", "created_at", "createdAt", "time", "ts", "@timestamp")

STOP = False


def _on_signal(signum, _frame):
    global STOP
    STOP = True
    log.info("Signal %s empfangen - beende nach aktuellem Batch", signum)


# --------------------------------------------------------------------------- config
def env(name, default=None, required=False):
    val = os.environ.get(name, default)
    if required and (val is None or str(val).strip() == ""):
        sys.exit(f"Fehlende Umgebungsvariable: {name}")
    return val


def env_int(name, default):
    return int(os.environ.get(name, default))


def env_bool(name, default):
    return str(os.environ.get(name, default)).strip().lower() in ("1", "true", "yes", "on")


def env_list(name, default=""):
    return [p.strip() for p in os.environ.get(name, default).split(",") if p.strip()]


class Cfg:
    def __init__(self):
        self.bucket = env("S3_BUCKET", required=True)
        self.prefixes = env_list("S3_PREFIXES") or [""]
        self.suffixes = tuple(env_list("S3_SUFFIXES", ".json,.jsonl,.ndjson,.json.gz,.jsonl.gz,.gz"))
        self.endpoint = env("S3_ENDPOINT_URL") or None
        self.region = env("AWS_REGION") or env("AWS_DEFAULT_REGION") or None
        self.path_style = env_bool("S3_FORCE_PATH_STYLE", False)

        self.host = env("SYSLOG_HOST", required=True)
        self.port = env_int("SYSLOG_PORT", 514)
        self.proto = env("SYSLOG_PROTOCOL", "tcp").lower()          # tcp | udp | tls
        self.framing = env("SYSLOG_FRAMING", "octet").lower()        # octet | newline
        self.fmt = env("SYSLOG_FORMAT", "rfc5424").lower()           # rfc5424 | rfc3164
        self.facility = FACILITIES[env("SYSLOG_FACILITY", "local0").lower()]
        self.severity = SEVERITIES[env("SYSLOG_SEVERITY", "info").lower()]
        self.app_name = env("SYSLOG_APP_NAME", "bifrost")[:48]
        self.hostname = (env("SYSLOG_HOSTNAME") or socket.gethostname())[:255]
        self.include_sd = env_bool("SYSLOG_INCLUDE_S3_SD", True)
        self.tls_ca = env("SYSLOG_TLS_CA_FILE") or None
        self.tls_verify = env_bool("SYSLOG_TLS_VERIFY", True)
        self.timeout = float(env("SYSLOG_TIMEOUT", "10"))
        self.retries = env_int("SYSLOG_RETRIES", 5)
        self.max_bytes = env_int("SYSLOG_MAX_MSG_BYTES", 0)          # 0 = kein Limit

        self.state_db = env("STATE_DB", "/data/state.db")
        self.state_s3 = env("STATE_S3_URI") or None                  # s3://bucket/key
        self.state_sync_s = env_int("STATE_SYNC_SECONDS", 60)
        self.commit_every = max(1, env_int("COMMIT_EVERY", 500))
        self.on_change = env("ON_OBJECT_CHANGE", "resume").lower()   # resume | resend | skip

        self.mode = env("RUN_MODE", "loop").lower()                  # loop | once
        self.interval = env_int("INTERVAL_SECONDS", 300)

        assert self.proto in ("tcp", "udp", "tls"), "SYSLOG_PROTOCOL: tcp|udp|tls"
        assert self.framing in ("octet", "newline"), "SYSLOG_FRAMING: octet|newline"
        assert self.fmt in ("rfc5424", "rfc3164"), "SYSLOG_FORMAT: rfc5424|rfc3164"
        assert self.on_change in ("resume", "resend", "skip"), "ON_OBJECT_CHANGE: resume|resend|skip"
        assert self.mode in ("loop", "once"), "RUN_MODE: loop|once"


# --------------------------------------------------------------------------- state
class State:
    def __init__(self, path):
        d = os.path.dirname(os.path.abspath(path))
        os.makedirs(d, exist_ok=True)
        self.path = path
        self.db = sqlite3.connect(path)
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS objects(
                   bucket TEXT NOT NULL,
                   key TEXT NOT NULL,
                   etag TEXT,
                   size INTEGER,
                   records_sent INTEGER NOT NULL DEFAULT 0,
                   done INTEGER NOT NULL DEFAULT 0,
                   updated_at TEXT,
                   PRIMARY KEY(bucket, key))"""
        )
        self.db.commit()

    def get(self, bucket, key):
        return self.db.execute(
            "SELECT etag, records_sent, done FROM objects WHERE bucket=? AND key=?", (bucket, key)
        ).fetchone()

    def save(self, bucket, key, etag, size, sent, done):
        self.db.execute(
            """INSERT INTO objects(bucket, key, etag, size, records_sent, done, updated_at)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(bucket, key) DO UPDATE SET
                 etag=excluded.etag, size=excluded.size, records_sent=excluded.records_sent,
                 done=excluded.done, updated_at=excluded.updated_at""",
            (bucket, key, etag, size, sent, int(done), datetime.datetime.now(UTC).isoformat()),
        )
        self.db.commit()

    def snapshot(self, dest):
        """Konsistente Kopie der DB (für Upload nach S3)."""
        out = sqlite3.connect(dest)
        with out:
            self.db.backup(out)
        out.close()


class StateSync:
    """Optional: State-DB in S3 sichern, damit der Container stateless laufen kann."""

    def __init__(self, s3, uri, local_path, interval):
        if not uri.startswith("s3://"):
            sys.exit("STATE_S3_URI muss mit s3:// beginnen")
        self.bucket, _, self.key = uri[5:].partition("/")
        self.s3, self.local, self.interval = s3, local_path, interval
        self.last = time.monotonic()

    def download(self):
        os.makedirs(os.path.dirname(os.path.abspath(self.local)), exist_ok=True)
        try:
            self.s3.download_file(self.bucket, self.key, self.local)
            log.info("State aus s3://%s/%s geladen", self.bucket, self.key)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                log.info("Noch kein State in S3 - starte neu")
            else:
                raise

    def upload(self, state, force=False):
        if not force and time.monotonic() - self.last < self.interval:
            return
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            state.snapshot(tmp_path)
            self.s3.upload_file(tmp_path, self.bucket, self.key)
            self.last = time.monotonic()
            log.debug("State nach S3 gesichert")
        finally:
            os.unlink(tmp_path)


# --------------------------------------------------------------------------- parsing
_NANO = re.compile(r"(\.\d{6})\d+")


def record_time(obj):
    if isinstance(obj, dict):
        for k in TS_KEYS:
            v = obj.get(k)
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                if v > 1e17:
                    v /= 1e9      # ns
                elif v > 1e14:
                    v /= 1e6      # µs
                elif v > 1e11:
                    v /= 1e3      # ms
                try:
                    return datetime.datetime.fromtimestamp(v, UTC)
                except (OverflowError, OSError, ValueError):
                    continue
            if isinstance(v, str):
                try:
                    d = datetime.datetime.fromisoformat(_NANO.sub(r"\1", v.strip()).replace("Z", "+00:00"))
                    return d if d.tzinfo else d.replace(tzinfo=UTC)
                except ValueError:
                    continue
    return datetime.datetime.now(UTC)


def iter_records(body: bytes):
    """Liefert (zeile_als_string, geparstes_objekt_oder_None) in fester Reihenfolge.
    Unterstützt: JSON-Array, einzelnes JSON-Objekt, JSON Lines; jeweils optional gzip."""
    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    text = body.decode("utf-8", errors="replace").lstrip("\ufeff").strip()
    if not text:
        return
    if text[0] in "[{":
        try:
            doc = json.loads(text)
        except json.JSONDecodeError:
            doc = None
        if isinstance(doc, list):
            for item in doc:
                yield json.dumps(item, ensure_ascii=False, separators=(",", ":")), item
            return
        if isinstance(doc, dict):
            yield json.dumps(doc, ensure_ascii=False, separators=(",", ":")), doc
            return
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            yield json.dumps(obj, ensure_ascii=False, separators=(",", ":")), obj
        except json.JSONDecodeError:
            log.warning("Ungültige JSON-Zeile wird roh gesendet: %.120s", line)
            yield line, None


# --------------------------------------------------------------------------- syslog
def _sd_escape(s):
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("]", "\\]")


class Formatter:
    def __init__(self, cfg):
        self.cfg = cfg
        self.pri = cfg.facility * 8 + cfg.severity

    def format(self, msg, ts, bucket, key):
        c = self.cfg
        if c.fmt == "rfc5424":
            t = ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            sd = (f'[s3@32473 bucket="{_sd_escape(bucket)}" key="{_sd_escape(key)}"]'
                  if c.include_sd else "-")
            line = f"<{self.pri}>1 {t} {c.hostname} {c.app_name} - - {sd} {msg}"
        else:
            lt = ts.astimezone()
            line = f"<{self.pri}>{lt:%b} {lt.day:2d} {lt:%H:%M:%S} {c.hostname} {c.app_name}: {msg}"
        data = line.encode("utf-8")
        if c.max_bytes and len(data) > c.max_bytes:
            log.warning("Nachricht aus %s gekürzt (%d > %d Bytes)", key, len(data), c.max_bytes)
            data = data[: c.max_bytes]
        return data


class Sender:
    def __init__(self, cfg):
        self.cfg = cfg
        self.sock = None

    def _connect(self):
        c = self.cfg
        if c.proto == "udp":
            fam, typ, proto, _, addr = socket.getaddrinfo(c.host, c.port, 0, socket.SOCK_DGRAM)[0]
            s = socket.socket(fam, typ, proto)
            s.connect(addr)
        else:
            s = socket.create_connection((c.host, c.port), timeout=c.timeout)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if c.proto == "tls":
                ctx = ssl.create_default_context(cafile=c.tls_ca)
                if not c.tls_verify:
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                s = ctx.wrap_socket(s, server_hostname=c.host)
        s.settimeout(c.timeout)
        self.sock = s
        log.debug("Verbunden mit %s:%s (%s)", c.host, c.port, c.proto)

    def _frame(self, data):
        if self.cfg.proto == "udp":
            return data
        if self.cfg.framing == "octet":
            return str(len(data)).encode() + b" " + data
        return data.replace(b"\n", b" ") + b"\n"

    unconfirmed = 0

    def send(self, data):
        """Sendet eine Nachricht. Bricht die Verbindung ab, nachdem im aktuellen Batch
        schon Daten gesendet wurden, wird BatchLost geworfen -> Aufrufer sendet den
        Batch ab dem letzten Commit erneut (at-least-once, kein Datenverlust)."""
        frame = self._frame(data)
        delay = 1.0
        for attempt in range(1, self.cfg.retries + 1):
            try:
                if self.sock is None:
                    self._connect()
                self.sock.sendall(frame)
                self.unconfirmed += 1
                return
            except OSError as e:
                log.warning("Senden fehlgeschlagen (Versuch %d/%d): %s", attempt, self.cfg.retries, e)
                self.close(quiet=True)
                if self.unconfirmed and self.cfg.proto != "udp":
                    self.unconfirmed = 0
                    raise BatchLost(str(e)) from e
            time.sleep(delay)
            delay = min(delay * 2, 30)
        raise ConnectionError(f"rsyslog nicht erreichbar nach {self.cfg.retries} Versuchen")

    def confirm(self):
        """Bestätigt, dass alle bisher gesendeten Daten beim Server angekommen sind.
        TCP: Schreibseite schließen und auf EOF des Servers warten (rsyslog hat alles gelesen).
        TLS/UDP: keine Bestätigung möglich -> nur sauber schließen."""
        self.unconfirmed = 0
        if self.sock is None:
            return
        try:
            if self.cfg.proto == "tcp":
                self.sock.shutdown(socket.SHUT_WR)
                while self.sock.recv(4096):
                    pass
            elif self.cfg.proto == "tls":
                try:
                    self.sock.unwrap()
                except (ssl.SSLError, OSError):
                    pass
        finally:
            self.close(quiet=True)

    def close(self, quiet=False):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                if not quiet:
                    raise
        self.sock = None


class BatchLost(Exception):
    pass


# --------------------------------------------------------------------------- core
class Shipper:
    def __init__(self, cfg, s3, state, sender, sync=None):
        self.cfg, self.s3, self.state, self.sender, self.sync = cfg, s3, state, sender, sync
        self.fmt = Formatter(cfg)

    def list_objects(self):
        pag = self.s3.get_paginator("list_objects_v2")
        for prefix in self.cfg.prefixes:
            for page in pag.paginate(Bucket=self.cfg.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    if k.endswith("/"):
                        continue
                    if self.cfg.suffixes and not k.lower().endswith(self.cfg.suffixes):
                        continue
                    yield obj

    def start_offset(self, key, etag, size):
        """Gibt den Start-Index zurück oder None, wenn nichts zu tun ist."""
        row = self.state.get(self.cfg.bucket, key)
        if not row:
            return 0
        old_etag, sent, done = row
        if old_etag == etag:
            return None if done else sent
        # Objekt wurde seit dem letzten Lauf überschrieben
        if self.cfg.on_change == "skip":
            log.info("%s geändert - ignoriert (ON_OBJECT_CHANGE=skip)", key)
            self.state.save(self.cfg.bucket, key, etag, size, sent, True)
            return None
        if self.cfg.on_change == "resend":
            log.info("%s geändert - wird komplett neu gesendet", key)
            return 0
        log.info("%s geändert - sende ab Datensatz %d (Append angenommen)", key, sent)
        return sent

    def process(self, obj):
        bucket, key = self.cfg.bucket, obj["Key"]
        etag, size = obj["ETag"].strip('"'), obj["Size"]
        start = self.start_offset(key, etag, size)
        if start is None:
            return 0
        try:
            body = self.s3.get_object(Bucket=bucket, Key=key, IfMatch=obj["ETag"])["Body"].read()
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("PreconditionFailed", "412"):
                log.info("%s wurde während des Lesens geändert - nächster Lauf", key)
                return 0
            raise

        records = list(iter_records(body))
        del body
        committed = start
        for attempt in range(1, self.cfg.retries + 1):
            try:
                pending = 0
                for idx in range(committed, len(records)):
                    line, parsed = records[idx]
                    self.sender.send(self.fmt.format(line, record_time(parsed), bucket, key))
                    pending += 1
                    if pending >= self.cfg.commit_every or STOP:
                        self.sender.confirm()
                        committed = idx + 1
                        pending = 0
                        self.state.save(bucket, key, etag, size, committed, False)
                        if self.sync:
                            self.sync.upload(self.state)
                        if STOP:
                            return committed - start
                if pending:
                    self.sender.confirm()
                break
            except (BatchLost, OSError) as e:
                # Verbindung im Batch abgebrochen / Bestätigung fehlgeschlagen:
                # ab letztem Commit neu senden
                self.sender.close(quiet=True)
                self.sender.unconfirmed = 0
                if attempt == self.cfg.retries:
                    raise ConnectionError(f"{key}: Batch konnte nicht zugestellt werden: {e}") from e
                log.warning("%s: Batch ab Datensatz %d wird neu gesendet (%s)", key, committed, e)
                time.sleep(min(2 ** attempt, 30))
        total = max(len(records), start)
        self.state.save(bucket, key, etag, size, total, True)
        if self.sync:
            self.sync.upload(self.state)
        sent = total - start
        if sent:
            log.info("%s: %d Datensätze gesendet", key, sent)
        return sent

    def run_cycle(self):
        t0, objs, recs, ok = time.monotonic(), 0, 0, True
        try:
            for obj in self.list_objects():
                if STOP:
                    break
                n = self.process(obj)
                if n:
                    objs += 1
                    recs += n
        except Exception as e:  # noqa: BLE001 - Lauf abbrechen, nächster Zyklus versucht erneut
            log.error("Lauf abgebrochen: %s", e, exc_info=log.isEnabledFor(logging.DEBUG))
            self.sender.close(quiet=True)
            ok = False
        log.info("Lauf beendet: %d Objekte, %d Datensätze, %.1fs", objs, recs, time.monotonic() - t0)
        return ok


def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    cfg = Cfg()

    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.endpoint,
        region_name=cfg.region,
        config=BotoConfig(
            retries={"max_attempts": 10, "mode": "standard"},
            s3={"addressing_style": "path" if cfg.path_style else "auto"},
        ),
    )
    sync = StateSync(s3, cfg.state_s3, cfg.state_db, cfg.state_sync_s) if cfg.state_s3 else None
    if sync:
        sync.download()
    state = State(cfg.state_db)
    sender = Sender(cfg)
    shipper = Shipper(cfg, s3, state, sender, sync)

    log.info("Start: s3://%s/%s -> %s://%s:%s (Modus %s)",
             cfg.bucket, ",".join(cfg.prefixes), cfg.proto, cfg.host, cfg.port, cfg.mode)
    ok = True
    while True:
        ok = shipper.run_cycle()
        if sync:
            sync.upload(state, force=True)
        if cfg.mode == "once" or STOP:
            break
        for _ in range(cfg.interval):
            if STOP:
                break
            time.sleep(1)
        if STOP:
            break
    sender.close(quiet=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
