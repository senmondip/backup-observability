# backup-observability

Prometheus, Grafana, and Loki tooling for monitoring backup and data-protection infrastructure. It exists because most backup platforms (commercial or homegrown) report success/failure inside their own console and nowhere else, which means backup health lives outside the same observability stack you use for everything else you operate. That gap is where missed backups, silent RPO breaches, and capacity exhaustion go unnoticed until a restore is needed and doesn't work. These tools pull backup job status, duration, and capacity data into Prometheus and Loki so that backup SLOs are visible, alertable, and reportable using the same stack as the rest of your infrastructure, instead of a separate login nobody checks until it's too late.

## Scripts

| Script | Purpose |
|---|---|
| `backup-metrics-exporter` | Prometheus exporter exposing job success rate, duration, and RPO attainment as metrics. |
| `grafana-backup-dashboard` | Provisioned Grafana dashboard covering success rate, RPO/RTO, and capacity. |
| `alertmanager-backup-rules` | Prometheus alerting rules for missed backups, RPO breach, and capacity exhaustion. |
| `loki-backup-log-parsing` | Promtail pipeline that extracts status codes from backup job logs for Loki. |
| `backup-slo-report` | Generates a monthly data-protection SLO report with trend and exception detail. |

## Requirements

- Prometheus (2.x) with a scrape target reachable for `backup-metrics-exporter`
- Grafana (9.x+) with provisioning enabled for dashboard JSON
- Alertmanager for routing rules produced by `alertmanager-backup-rules`
- Loki + Promtail for log-based parsing
- Python 3.9+ (exporter and report generator)
- Read access to your backup platform's job/status source (API, CLI, or log output) — you will need to adapt the collection layer in `backup-metrics-exporter` to your specific backup product

## Usage

1. Configure `backup-metrics-exporter` with your backup platform's job status source and run it as a long-lived process or sidecar; point Prometheus at it as a scrape target.
2. Load the rule files from `alertmanager-backup-rules` into your Prometheus rule path and reload.
3. Import the dashboard JSON from `grafana-backup-dashboard` via Grafana provisioning or the UI; point its datasource variable at your Prometheus instance.
4. Point Promtail at your backup job logs using the pipeline in `loki-backup-log-parsing`, then ship to Loki.
5. Run `backup-slo-report` on a monthly schedule (cron, systemd timer, or CI job) against your Prometheus/Loki endpoints to produce the SLO report.

Each script directory has its own configuration file with the specific fields you need to fill in for your environment (job label names, thresholds, retention targets, etc.).

## Provenance

These tools are generalised from roughly 20 years of enterprise data-protection engineering work. Metric names, thresholds, and dashboard layouts reflect patterns seen across many environments, not any single one. There is no customer data, site-specific configuration, or identifying information in this repository — everything here needs to be adapted to your own backup platform and environment before use.
