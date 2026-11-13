# Dashboards

`grafana-infinity-datasource-cache.json` — the service dashboard, eight tabs: Overview, Clients, Keys, Refreshes,
Upstreams, Redis, Runtime, Logs. Every panel has a description with what is normal and what to do.

## Import

The file is a dashboard in the v2 schema (`dashboard.grafana.app/v2`), so it needs Grafana 12 or newer with the
new dashboard schema enabled. Dashboards → New → Import → paste the file. Grafana picks a new UID.

After the import choose the datasources in the `Metrics` (Prometheus) and `Logs` (Loki) variables.

## What it expects

| Data | Used by | |
|---|---|---|
| `datasource_cache_*`, `go_*`, `process_*` from `/metrics` | all tabs | e.g. the chart's `serviceMonitor.enabled: true` |
| kube-state-metrics | Replicas ready, restarts, CPU/memory limits | |
| cAdvisor (`container_*`) | CPU, memory, throttling | duplicates from kubelet `/metrics/resource` are filtered out by `metrics_path` |
| Loki with the labels `namespace` and `pod` | Logs tab | the service must log JSON: `log.format: json` (the chart default) |

- `Cluster` and `Namespace` come from the `cluster` and `namespace` labels of `datasource_cache_build_info`. Without
  these labels (outside Kubernetes, a single Prometheus) the variables stay empty and match everything.
- `Deployment` is picked from kube-state-metrics by a name containing `datasource-cache`; with a custom
  `fullnameOverride` change the variable's regex.
- Without Redis the Redis tab is empty, and so is the `Redis` tile — expected.
- Thresholds are relative to the service configuration exported as metrics (`ttl`, `max_stale`, `max_keys`,
  `concurrency`, …), so nothing needs tuning per deployment.
