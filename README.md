# backup-observability

Prometheus, Grafana, and Loki configuration for backup and data-protection monitoring.

The scripts collect backup job status, duration, and capacity data from backup platform logs and job output, then expose them as Prometheus metrics, Grafana panels, and alerting rules. They expect backup job logs or job-run output as input and require a running Prometheus, Grafana, and Loki/Promtail stack to attach to.

## Scripts

| Script | Purpose |
|---|---|
| `backup-metrics-exporter` | Exposes job success rate, duration, and RPO attainment as Prometheus metrics. |
| `grafana-backup-dashboard` | Provisions a Grafana dashboard for success rate, RPO/RTO, and capacity. |
| `alertmanager-backup-rules` | Prometheus alerting rules for missed backups, RPO breach, and capacity exhaustion. |
| `loki-backup-log-parsing` | Promtail pipeline extracting status codes from backup logs. |
| `backup-slo-report` | Generates a monthly data-protection SLO report with trend and exception detail. |

## Requirements

- Prometheus and Alertmanager
- Grafana with provisioning enabled
- Loki and Promtail
- Python 3.9+ (for `backup-metrics-exporter` and `backup-slo-report`)
- Access to backup job logs or backup platform API/CLI output

## Usage

1. Point `backup-metrics-exporter` at your backup job logs or API and run it as a scrape target for Prometheus.
2. Load `alertmanager-backup-rules` into Prometheus rule files and reload.
3. Import `grafana-backup-dashboard` into Grafana via provisioning or the UI.
4. Add `loki-backup-log-parsing` to your Promtail config to extract status codes from backup logs into Loki.
5. Run `backup-slo-report` on a monthly schedule to generate the SLO report.

These scripts are generalised from production data-protection work and contain no customer data or site-specific configuration.
