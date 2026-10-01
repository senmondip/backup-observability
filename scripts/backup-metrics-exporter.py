#!/usr/bin/env python3
"""
backup-metrics-exporter
========================

WHAT IT DOES
------------
Reads backup job run records (one record per attempted backup job run) from
one or more sources, computes:

  * job success rate over a rolling window of recent runs
  * duration of the most recently completed run
  * RPO ("Recovery Point Objective") attainment -- how old the last
    successful backup is versus a per-job or default target

...and exposes the results as Prometheus metrics, either by:

  * serving them over HTTP for Prometheus to scrape (--mode serve)
  * writing a .prom file for the node_exporter textfile collector
    (--mode textfile, intended to be run from cron/systemd timer)
  * printing them once to stdout (--mode report, the default), as a
    human-readable table or, with --json, a machine-readable JSON report

WHAT IT ASSUMES
----------------
  * This script does not know about any specific backup product (Veeam,
    Bacula, restic, TSM, ...). It expects job run data as JSON, either:
      - a local file / glob pattern containing a JSON array of run records,
        or a single run record object, or
      - an HTTP(S) endpoint returning a JSON array of run records.
    Point your backup system's reporting/export feature (or a small glue
    script) at producing that JSON; this exporter only consumes it.

  * Each run record is a JSON object with (at minimum):
        {
          "job_name":  "nightly-db-dump",       // required, string
          "status":    "success",               // required: success|failure|warning|running
                                                  //   (aliases ok/succeeded, failed/error, warn,
                                                  //    running/in_progress are also accepted)
          "start_time": "2026-08-18T01:00:00Z",  // required, ISO-8601 or unix epoch seconds
          "end_time":   "2026-08-18T01:12:30Z",  // optional (absent = still running)
          "rpo_target_seconds": 43200            // optional, overrides --default-rpo-target-seconds
        }
    Records that don't parse are skipped (counted and reported), not fatal,
    unless a source yields zero usable records overall.

  * No hostnames, credentials, file paths, or customer identifiers are
    baked into this script. Everything comes from --flags or environment
    variables (see --help epilog).

HOW TO RUN IT
-------------
  # One-off human-readable report, exit code reflects health:
  ./backup-metrics-exporter.py --source /var/backup-reports/*.json

  # Same, but as JSON (e.g. for piping into another tool):
  ./backup-metrics-exporter.py --source /var/backup-reports/*.json --json

  # Pull from an HTTP API (auth via env vars, see --help):
  export BACKUP_METRICS_API_TOKEN=xxxxx
  ./backup-metrics-exporter.py --source https://backup-api.example.internal/runs

  # Write a node_exporter textfile-collector file (run this from cron):
  ./backup-metrics-exporter.py --source /var/backup-reports/*.json \
      --mode textfile --textfile-dir /var/lib/node_exporter/textfile_collector

  # Run as a long-lived Prometheus exporter:
  ./backup-metrics-exporter.py --source https://backup-api.example.internal/runs \
      --mode serve --listen-address 0.0.0.0:9109

Exit codes (report / textfile modes): 0 = all jobs healthy, 2 = data was
collected but at least one job is failing or has breached its RPO target,
1 = could not collect data at all (bad source, network/auth failure, etc).
`--mode serve` runs until interrupted and only uses exit code 1, for
startup failures.

Requires only the Python 3 standard library (tested on 3.9+).
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import glob
import json
import logging
import os
import signal
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple

METRIC_PREFIX = "backup_job_"
DEFAULT_LISTEN_ADDRESS = "0.0.0.0:9109"

log = logging.getLogger("backup-metrics-exporter")


# --------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------

class SourceError(Exception):
    """Raised when a data source can't be reached/read at all (fatal)."""


class DataFormatError(Exception):
    """Raised when a source is reachable but yields no usable records (fatal)."""


# --------------------------------------------------------------------------
# Data model
# --------------------------------------------------------------------------

_STATUS_ALIASES = {
    "success": "success", "ok": "success", "succeeded": "success",
    "failure": "failure", "failed": "failure", "error": "failure",
    "warning": "warning", "warn": "warning",
    "running": "running", "in_progress": "running", "in-progress": "running",
}


@dataclasses.dataclass
class JobRun:
    job_name: str
    status: str  # normalized: success|failure|warning|running
    start_time: datetime
    end_time: Optional[datetime]
    rpo_target_seconds: Optional[float]
    origin: str  # where this record came from, for error messages


@dataclasses.dataclass
class JobMetrics:
    job_name: str
    success_rate: Optional[float]
    runs_evaluated: int
    last_run_success: Optional[bool]
    last_run_duration_seconds: Optional[float]
    last_success_timestamp: Optional[datetime]
    rpo_target_seconds: float
    rpo_age_seconds: Optional[float]
    rpo_attained: bool

    @property
    def healthy(self) -> bool:
        return bool(self.last_run_success) and self.rpo_attained


# --------------------------------------------------------------------------
# Timestamp parsing
# --------------------------------------------------------------------------

def parse_timestamp(value: Any) -> datetime:
    """Accept unix epoch seconds (int/float/numeric string) or ISO-8601
    (with or without a trailing 'Z', which datetime.fromisoformat only
    started accepting natively in 3.11 -- we normalize it ourselves so this
    also works on 3.9/3.10)."""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        try:
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        except ValueError:
            pass
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"unparseable timestamp: {value!r}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    raise ValueError(f"unparseable timestamp: {value!r}")


def normalize_record(raw: Dict[str, Any], origin: str, default_rpo: float) -> JobRun:
    if not isinstance(raw, dict):
        raise ValueError("record is not a JSON object")

    job_name = raw.get("job_name")
    if not job_name or not isinstance(job_name, str):
        raise ValueError("missing/invalid 'job_name'")

    raw_status = str(raw.get("status", "")).strip().lower()
    status = _STATUS_ALIASES.get(raw_status)
    if status is None:
        raise ValueError(f"job {job_name!r}: unrecognized status {raw.get('status')!r}")

    if "start_time" not in raw:
        raise ValueError(f"job {job_name!r}: missing 'start_time'")
    start_time = parse_timestamp(raw["start_time"])

    end_time = None
    if raw.get("end_time") is not None:
        end_time = parse_timestamp(raw["end_time"])

    rpo_target = raw.get("rpo_target_seconds")
    if rpo_target is not None:
        try:
            rpo_target = float(rpo_target)
        except (TypeError, ValueError):
            raise ValueError(f"job {job_name!r}: invalid rpo_target_seconds {rpo_target!r}")

    return JobRun(
        job_name=job_name,
        status=status,
        start_time=start_time,
        end_time=end_time,
        rpo_target_seconds=rpo_target,
        origin=origin,
    )


# --------------------------------------------------------------------------
# Loading records from sources
# --------------------------------------------------------------------------

def _records_from_json_blob(blob: Any) -> List[Any]:
    if isinstance(blob, list):
        return blob
    if isinstance(blob, dict):
        return [blob]
    raise DataFormatError("expected a JSON object or array of run records")


def load_from_file_source(pattern: str) -> List[Tuple[Any, str]]:
    """Expand a glob pattern and load each matched file as JSON. Returns
    (raw_record, origin_description) pairs so bad records can be traced
    back to a specific file."""
    paths = sorted(glob.glob(os.path.expanduser(pattern)))
    if not paths:
        raise SourceError(f"no files matched pattern: {pattern!r}")

    out: List[Tuple[Any, str]] = []
    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as fh:
                blob = json.load(fh)
        except OSError as exc:
            raise SourceError(f"cannot read {path}: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise DataFormatError(f"{path}: invalid JSON ({exc})") from exc
        for record in _records_from_json_blob(blob):
            out.append((record, path))
    return out


def _build_ssl_context(insecure: bool) -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    if insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def load_from_url_source(url: str, timeout: float, insecure: bool) -> List[Tuple[Any, str]]:
    """Fetch run records from an HTTP(S) API. Auth is intentionally not a
    CLI flag: credentials belong in the environment, not in argv (which is
    visible via `ps`) or shell history."""
    headers = {"Accept": "application/json"}

    token = os.environ.get("BACKUP_METRICS_API_TOKEN")
    basic_user = os.environ.get("BACKUP_METRICS_BASIC_AUTH_USER")
    basic_pass = os.environ.get("BACKUP_METRICS_BASIC_AUTH_PASS")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif basic_user and basic_pass:
        creds = base64.b64encode(f"{basic_user}:{basic_pass}".encode("utf-8")).decode("ascii")
        headers["Authorization"] = f"Basic {creds}"
    else:
        log.warning(
            "no credentials found for %s (set BACKUP_METRICS_API_TOKEN or "
            "BACKUP_METRICS_BASIC_AUTH_USER/PASS if this endpoint requires auth)",
            url,
        )

    request = urllib.request.Request(url, headers=headers, method="GET")
    ctx = _build_ssl_context(insecure) if url.lower().startswith("https://") else None

    try:
        with urllib.request.urlopen(request, timeout=timeout, context=ctx) as resp:
            body = resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read(500).decode("utf-8", "replace") if hasattr(exc, "read") else ""
        raise SourceError(f"{url}: HTTP {exc.code} {exc.reason} {detail}".strip()) from exc
    except urllib.error.URLError as exc:
        raise SourceError(f"{url}: {exc.reason}") from exc
    except TimeoutError as exc:
        raise SourceError(f"{url}: timed out after {timeout}s") from exc

    try:
        blob = json.loads(body)
    except json.JSONDecodeError as exc:
        raise DataFormatError(f"{url}: response was not valid JSON ({exc})") from exc

    return [(record, url) for record in _records_from_json_blob(blob)]


def load_all_sources(sources: List[str], timeout: float, insecure: bool) -> Tuple[List[JobRun], int]:
    """Load and normalize records from every configured source. Returns the
    usable JobRuns plus a count of records that were skipped for being
    malformed. Raises SourceError/DataFormatError only for hard failures
    (unreachable source, or zero usable records overall)."""
    raw_pairs: List[Tuple[Any, str]] = []
    for source in sources:
        if source.lower().startswith(("http://", "https://")):
            raw_pairs.extend(load_from_url_source(source, timeout, insecure))
        else:
            raw_pairs.extend(load_from_file_source(source))

    runs: List[JobRun] = []
    skipped = 0
    for raw, origin in raw_pairs:
        try:
            runs.append(normalize_record(raw, origin, default_rpo=0.0))
        except ValueError as exc:
            skipped += 1
            log.warning("skipping malformed record from %s: %s", origin, exc)

    if not runs:
        raise DataFormatError(
            f"loaded 0 usable run records from {len(sources)} source(s) "
            f"({skipped} record(s) were malformed and skipped)"
        )
    return runs, skipped


# --------------------------------------------------------------------------
# Metric computation
# --------------------------------------------------------------------------

def compute_job_metrics(
    runs: List[JobRun], window_runs: int, default_rpo_target_seconds: float, now: datetime
) -> JobMetrics:
    job_name = runs[0].job_name

    # Success rate: over the most recent `window_runs` *completed* runs
    # (i.e. excluding still-running ones). "warning" counts as not-success
    # here, on the theory that a warning means the backup needs a human
    # to look at it even if it technically finished.
    completed = sorted(
        (r for r in runs if r.status != "running"), key=lambda r: r.start_time, reverse=True
    )
    window = completed[:window_runs]
    if window:
        successes = sum(1 for r in window if r.status == "success")
        success_rate = successes / len(window)
    else:
        success_rate = None

    last_run_success = window[0].status == "success" if window else None
    last_run_duration_seconds = None
    if window and window[0].end_time is not None:
        last_run_duration_seconds = (window[0].end_time - window[0].start_time).total_seconds()

    # RPO is about the most recent *successful* backup, searched across all
    # known history for this job, not just the recent window -- a job that
    # has failed its last 5 runs but succeeded 6 runs ago still has a real
    # (aging) recovery point.
    successful_runs = [r for r in runs if r.status == "success" and r.end_time is not None]
    last_success = max(successful_runs, key=lambda r: r.end_time) if successful_runs else None

    # Per-job RPO target wins if any run record specified one; otherwise
    # fall back to the operator-supplied default.
    job_rpo_target = next(
        (r.rpo_target_seconds for r in runs if r.rpo_target_seconds is not None),
        default_rpo_target_seconds,
    )

    if last_success is not None:
        rpo_age_seconds = (now - last_success.end_time).total_seconds()
        rpo_attained = rpo_age_seconds <= job_rpo_target
    else:
        rpo_age_seconds = None
        rpo_attained = False  # never succeeded => by definition RPO is not met

    return JobMetrics(
        job_name=job_name,
        success_rate=success_rate,
        runs_evaluated=len(window),
        last_run_success=last_run_success,
        last_run_duration_seconds=last_run_duration_seconds,
        last_success_timestamp=last_success.end_time if last_success else None,
        rpo_target_seconds=job_rpo_target,
        rpo_age_seconds=rpo_age_seconds,
        rpo_attained=rpo_attained,
    )


def collect(args: argparse.Namespace, now: datetime) -> Tuple[List[JobMetrics], int]:
    runs, skipped = load_all_sources(args.source, args.timeout, args.insecure)
    by_job: Dict[str, List[JobRun]] = {}
    for run in runs:
        by_job.setdefault(run.job_name, []).append(run)
    metrics = [
        compute_job_metrics(job_runs, args.window_runs, args.default_rpo_target_seconds, now)
        for job_runs in by_job.values()
    ]
    metrics.sort(key=lambda m: m.job_name)
    return metrics, skipped


# --------------------------------------------------------------------------
# Rendering: Prometheus exposition format
# --------------------------------------------------------------------------

def _escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render_prometheus_text(
    metrics: Optional[List[JobMetrics]],
    skipped: int,
    scrape_ok: bool,
    scrape_timestamp: datetime,
    error: Optional[str] = None,
) -> str:
    lines: List[str] = []

    def emit(name: str, help_text: str, metric_type: str, samples: List[Tuple[Dict[str, str], Any]]):
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {metric_type}")
        for labels, value in samples:
            if labels:
                label_str = ",".join(f'{k}="{_escape_label_value(v)}"' for k, v in labels.items())
                lines.append(f"{name}{{{label_str}}} {value}")
            else:
                lines.append(f"{name} {value}")

    emit(
        "backup_exporter_last_scrape_success",
        "Whether the most recent collection of backup data succeeded (1) or failed (0).",
        "gauge",
        [({}, 1 if scrape_ok else 0)],
    )
    emit(
        "backup_exporter_last_scrape_timestamp_seconds",
        "Unix timestamp of the most recent collection attempt.",
        "gauge",
        [({}, scrape_timestamp.timestamp())],
    )
    if error:
        lines.append(f"# backup_exporter_last_scrape_error: {error}")

    if metrics is not None:
        emit(
            "backup_exporter_jobs_total",
            "Number of distinct backup jobs seen in the last collection.",
            "gauge",
            [({}, len(metrics))],
        )
        emit(
            "backup_exporter_data_quality_issues_total",
            "Number of malformed run records skipped in the last collection.",
            "gauge",
            [({}, skipped)],
        )

        emit(
            f"{METRIC_PREFIX}success_rate",
            "Fraction of recent runs (within the evaluation window) that succeeded.",
            "gauge",
            [({"job": m.job_name}, m.success_rate) for m in metrics if m.success_rate is not None],
        )
        emit(
            f"{METRIC_PREFIX}runs_evaluated",
            "Number of completed runs considered for the success-rate calculation.",
            "gauge",
            [({"job": m.job_name}, m.runs_evaluated) for m in metrics],
        )
        emit(
            f"{METRIC_PREFIX}last_run_success",
            "Whether the most recent completed run succeeded (1) or not (0).",
            "gauge",
            [({"job": m.job_name}, 1 if m.last_run_success else 0)
             for m in metrics if m.last_run_success is not None],
        )
        emit(
            f"{METRIC_PREFIX}last_run_duration_seconds",
            "Duration of the most recently completed run, in seconds.",
            "gauge",
            [({"job": m.job_name}, m.last_run_duration_seconds)
             for m in metrics if m.last_run_duration_seconds is not None],
        )
        emit(
            f"{METRIC_PREFIX}last_success_timestamp_seconds",
            "Unix timestamp of the end of the most recent successful run.",
            "gauge",
            [({"job": m.job_name}, m.last_success_timestamp.timestamp())
             for m in metrics if m.last_success_timestamp is not None],
        )
        emit(
            f"{METRIC_PREFIX}rpo_target_seconds",
            "Configured Recovery Point Objective for this job, in seconds.",
            "gauge",
            [({"job": m.job_name}, m.rpo_target_seconds) for m in metrics],
        )
        emit(
            f"{METRIC_PREFIX}rpo_age_seconds",
            "Age of the most recent successful backup relative to the collection time, in seconds.",
            "gauge",
            [({"job": m.job_name}, m.rpo_age_seconds) for m in metrics if m.rpo_age_seconds is not None],
        )
        emit(
            f"{METRIC_PREFIX}rpo_attained",
            "Whether the last successful backup is within the RPO target (1) or not (0).",
            "gauge",
            [({"job": m.job_name}, 1 if m.rpo_attained else 0) for m in metrics],
        )

    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------
# Rendering: human report / JSON report
# --------------------------------------------------------------------------

def render_human_report(metrics: List[JobMetrics], skipped: int) -> str:
    if not metrics:
        return "No backup jobs found in the configured source(s).\n"

    headers = ("JOB", "LAST", "SUCCESS%", "DURATION", "RPO TARGET", "RPO AGE", "RPO OK")
    rows = []
    for m in metrics:
        last = "success" if m.last_run_success else ("failure" if m.last_run_success is not None else "n/a")
        success_pct = f"{m.success_rate * 100:.0f}%" if m.success_rate is not None else "n/a"
        duration = f"{m.last_run_duration_seconds:.0f}s" if m.last_run_duration_seconds is not None else "n/a"
        target = f"{m.rpo_target_seconds:.0f}s"
        age = f"{m.rpo_age_seconds:.0f}s" if m.rpo_age_seconds is not None else "never"
        ok = "OK" if m.rpo_attained else "BREACHED"
        rows.append((m.job_name, last, success_pct, duration, target, age, ok))

    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    lines = ["  ".join(h.ljust(w) for h, w in zip(headers, widths))]
    lines.append("  ".join("-" * w for w in widths))
    for row in rows:
        lines.append("  ".join(c.ljust(w) for c, w in zip(row, widths)))

    unhealthy = [m.job_name for m in metrics if not m.healthy]
    lines.append("")
    if unhealthy:
        lines.append(f"ACTION NEEDED: {len(unhealthy)} job(s) unhealthy: {', '.join(unhealthy)}")
    else:
        lines.append("All jobs healthy.")
    if skipped:
        lines.append(f"NOTE: {skipped} malformed run record(s) were skipped -- check source data quality.")

    return "\n".join(lines) + "\n"


def render_json_report(metrics: List[JobMetrics], skipped: int, now: datetime) -> str:
    payload = {
        "collected_at": now.isoformat(),
        "data_quality_issues_skipped": skipped,
        "jobs": [
            {
                "job_name": m.job_name,
                "success_rate": m.success_rate,
                "runs_evaluated": m.runs_evaluated,
                "last_run_success": m.last_run_success,
                "last_run_duration_seconds": m.last_run_duration_seconds,
                "last_success_timestamp": m.last_success_timestamp.isoformat() if m.last_success_timestamp else None,
                "rpo_target_seconds": m.rpo_target_seconds,
                "rpo_age_seconds": m.rpo_age_seconds,
                "rpo_attained": m.rpo_attained,
                "healthy": m.healthy,
            }
            for m in metrics
        ],
        "overall_healthy": all(m.healthy for m in metrics) if metrics else False,
    }
    return json.dumps(payload, indent=2) + "\n"


# --------------------------------------------------------------------------
# HTTP server (--mode serve)
# --------------------------------------------------------------------------

def _make_handler(args: argparse.Namespace):
    class MetricsHandler(BaseHTTPRequestHandler):
        server_version = "backup-metrics-exporter/1.0"

        def log_message(self, fmt, *fmt_args):  # route through logging, not stderr directly
            log.info("%s - %s", self.address_string(), fmt % fmt_args)

        def do_GET(self):
            if self.path in ("/", ""):
                self._respond(200, "text/plain", "backup-metrics-exporter: see /metrics\n")
                return
            if self.path == "/healthz":
                self._respond(200, "text/plain", "ok\n")
                return
            if self.path != "/metrics":
                self._respond(404, "text/plain", "not found\n")
                return

            now = datetime.now(timezone.utc)
            try:
                metrics, skipped = collect(args, now)
            except (SourceError, DataFormatError) as exc:
                log.error("collection failed: %s", exc)
                # Return 200 with a failure gauge rather than 5xx: this lets
                # alerting rules live inside Prometheus (on
                # backup_exporter_last_scrape_success) instead of relying
                # solely on the scrape-level `up` metric.
                body = render_prometheus_text(None, 0, scrape_ok=False, scrape_timestamp=now, error=str(exc))
                self._respond(200, "text/plain; version=0.0.4; charset=utf-8", body)
                return

            body = render_prometheus_text(metrics, skipped, scrape_ok=True, scrape_timestamp=now)
            self._respond(200, "text/plain; version=0.0.4; charset=utf-8", body)

        def _respond(self, status: int, content_type: str, body: str):
            encoded = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    return MetricsHandler


def run_server(args: argparse.Namespace) -> int:
    host, _, port_str = args.listen_address.rpartition(":")
    if not host or not port_str.isdigit():
        log.error("invalid --listen-address %r, expected HOST:PORT", args.listen_address)
        return 1
    port = int(port_str)

    try:
        httpd = ThreadingHTTPServer((host, port), _make_handler(args))
    except OSError as exc:
        log.error("cannot bind %s:%s: %s", host, port, exc)
        return 1

    def _shutdown(signum, _frame):
        log.info("received signal %s, shutting down", signum)
        httpd.shutdown()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("serving backup metrics on http://%s:%s/metrics", host, port)
    httpd.serve_forever()
    httpd.server_close()
    return 0


# --------------------------------------------------------------------------
# Textfile mode (--mode textfile)
# --------------------------------------------------------------------------

def run_textfile(args: argparse.Namespace, now: datetime) -> int:
    if not args.textfile_dir:
        log.error("--mode textfile requires --textfile-dir")
        return 1
    if not os.path.isdir(args.textfile_dir):
        log.error("--textfile-dir %r does not exist or is not a directory", args.textfile_dir)
        return 1

    filename = args.filename
    if not filename.endswith(".prom"):
        log.warning("--filename %r does not end in .prom; node_exporter's textfile "
                     "collector will ignore it", filename)

    target_path = os.path.join(args.textfile_dir, filename)
    tmp_path = f"{target_path}.tmp.{os.getpid()}"

    try:
        metrics, skipped = collect(args, now)
    except (SourceError, DataFormatError) as exc:
        log.error("collection failed: %s", exc)
        body = render_prometheus_text(None, 0, scrape_ok=False, scrape_timestamp=now, error=str(exc))
        _atomic_write(tmp_path, target_path, body)
        return 1

    body = render_prometheus_text(metrics, skipped, scrape_ok=True, scrape_timestamp=now)
    _atomic_write(tmp_path, target_path, body)
    log.info("wrote %s (%d jobs)", target_path, len(metrics))
    sys.stderr.write(render_human_report(metrics, skipped))

    return 0 if all(m.healthy for m in metrics) else 2


def _atomic_write(tmp_path: str, target_path: str, body: str) -> None:
    # Write-then-rename avoids node_exporter's textfile collector ever
    # reading a half-written file.
    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write(body)
        os.replace(tmp_path, target_path)
    except OSError as exc:
        raise SourceError(f"cannot write {target_path}: {exc}") from exc


# --------------------------------------------------------------------------
# Report mode (--mode report, the default)
# --------------------------------------------------------------------------

def run_report(args: argparse.Namespace, now: datetime) -> int:
    try:
        metrics, skipped = collect(args, now)
    except (SourceError, DataFormatError) as exc:
        log.error("collection failed: %s", exc)
        if args.json:
            print(json.dumps({"error": str(exc)}, indent=2))
        else:
            print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(render_json_report(metrics, skipped, now), end="")
    else:
        print(render_human_report(metrics, skipped), end="")

    return 0 if all(m.healthy for m in metrics) else 2


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backup-metrics-exporter",
        description="Prometheus exporter for backup job success rate, duration, and RPO attainment.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Environment variables:\n"
            "  BACKUP_METRICS_SOURCE           default for --source (comma-separated)\n"
            "  BACKUP_METRICS_API_TOKEN        Bearer token for HTTP(S) sources\n"
            "  BACKUP_METRICS_BASIC_AUTH_USER  Basic auth username for HTTP(S) sources\n"
            "  BACKUP_METRICS_BASIC_AUTH_PASS  Basic auth password for HTTP(S) sources\n"
            "\nCredentials are read from the environment only, never from flags, so they\n"
            "don't leak via `ps` or shell history.\n"
        ),
    )
    parser.add_argument(
        "--source", action="append", metavar="PATH_OR_URL",
        help="A local file/glob pattern (e.g. '/var/backup-reports/*.json') or an "
             "http(s):// URL returning JSON run records. Repeatable. Falls back to "
             "the comma-separated BACKUP_METRICS_SOURCE env var if omitted.",
    )
    parser.add_argument(
        "--mode", choices=["report", "textfile", "serve"], default="report",
        help="report: print once and exit (default). textfile: write a "
             "node_exporter textfile-collector file and exit. serve: run an "
             "HTTP server exposing /metrics.",
    )
    parser.add_argument("--json", action="store_true",
                         help="In --mode report, print a JSON report instead of a text table.")
    parser.add_argument("--textfile-dir", metavar="DIR",
                         help="Directory to write the .prom file into (required for --mode textfile).")
    parser.add_argument("--filename", default="backup_metrics.prom",
                         help="Filename to write within --textfile-dir (default: %(default)s).")
    parser.add_argument("--listen-address", default=DEFAULT_LISTEN_ADDRESS, metavar="HOST:PORT",
                         help="Address to bind for --mode serve (default: %(default)s).")
    parser.add_argument("--default-rpo-target-seconds", type=float, default=86400.0, metavar="SECONDS",
                         help="RPO target used for jobs that don't specify their own "
                              "rpo_target_seconds (default: %(default)s = 24h).")
    parser.add_argument("--window-runs", type=int, default=10, metavar="N",
                         help="Number of most recent completed runs per job to use for "
                              "the success-rate calculation (default: %(default)s).")
    parser.add_argument("--timeout", type=float, default=10.0, metavar="SECONDS",
                         help="Timeout for HTTP(S) source requests (default: %(default)s).")
    parser.add_argument("--insecure", action="store_true",
                         help="Skip TLS certificate verification for https:// sources. Do not "
                              "use this against untrusted networks.")
    parser.add_argument("--log-level", default="INFO",
                         choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                         help="Logging verbosity (default: %(default)s).")

    args = parser.parse_args(argv)

    if not args.source:
        env_source = os.environ.get("BACKUP_METRICS_SOURCE", "")
        args.source = [s.strip() for s in env_source.split(",") if s.strip()]
    if not args.source:
        parser.error("no data source given: use --source PATH_OR_URL (repeatable) "
                      "or set BACKUP_METRICS_SOURCE")
    if args.window_runs < 1:
        parser.error("--window-runs must be >= 1")
    if args.mode == "textfile" and not args.textfile_dir:
        parser.error("--mode textfile requires --textfile-dir")
    if args.json and args.mode != "report":
        parser.error("--json is only meaningful with --mode report")

    return args


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if args.mode == "serve":
        return run_server(args)

    now = datetime.now(timezone.utc)
    if args.mode == "textfile":
        return run_textfile(args, now)
    return run_report(args, now)


if __name__ == "__main__":
    sys.exit(main())
