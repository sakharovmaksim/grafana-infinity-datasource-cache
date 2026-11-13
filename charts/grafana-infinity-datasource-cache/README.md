# grafana-infinity-datasource-cache chart

Deploys [grafana-infinity-datasource-cache](https://github.com/sakharovmaksim/grafana-infinity-datasource-cache): a caching proxy for HTTP APIs queried by the Grafana
Infinity datasource.

```
helm install infinity-cache oci://ghcr.io/sakharovmaksim/charts/grafana-infinity-datasource-cache \
  --namespace grafana --values my-values.yaml
```

`config` is the service configuration in the format of
[config.example.yml](https://github.com/sakharovmaksim/grafana-infinity-datasource-cache/blob/master/config.example.yml); at least one source is required:

```yaml
config:
  sources:
    osv:
      url: https://api.osv.dev
      paths: [/v1/query, /v1/querybatch]
      methods: [POST]
    endoflife:
      url: https://endoflife.date
      paths: [/api/]
      warmup:
        - /api/go.json
```

The datasource URLs are printed after install: `http://<release>-grafana-infinity-datasource-cache.<namespace>.svc.cluster.local/<source>`
(`fullnameOverride` shortens it).

## API keys

From a Secret through an environment variable, referenced as `${VAR}` in the config:

```yaml
extraEnv:
  - name: NVD_API_KEY
    valueFrom:
      secretKeyRef: {name: nvd-api-key, key: api-key}
config:
  sources:
    nvd:
      url: https://services.nvd.nist.gov
      paths: [/rest/json/]
      auth: {header: apiKey, value: "${NVD_API_KEY}"}
```

Or as a file, which is re-read on every request, so a rotated key needs no restart:

```yaml
extraVolumes:
  - name: nvd-api-key
    secret: {secretName: nvd-api-key}
extraVolumeMounts:
  - name: nvd-api-key
    mountPath: /etc/nvd
    readOnly: true
config:
  sources:
    nvd:
      url: https://services.nvd.nist.gov
      paths: [/rest/json/]
      auth: {header: apiKey, value_file: /etc/nvd/api-key}
```

## Several replicas

Replicas need a shared Redis, otherwise each one fetches and keeps its own copies. The chart does not install Redis.
Rate limits (`rate`) apply per replica: divide the upstream limit by the replica count.

```yaml
replicaCount: 2
config:
  redis:
    addr: redis-master.redis.svc.cluster.local:6379
    db: 1
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
        app.kubernetes.io/name: grafana-infinity-datasource-cache
```

## Security

The proxy adds upstream API keys to requests, so anyone who reaches it uses them. The chart has no Ingress on
purpose; `networkPolicy` limits callers to the listed peers (Grafana and the metrics scraper):

```yaml
networkPolicy:
  enabled: true
  from:
    - namespaceSelector:
        matchLabels: {kubernetes.io/metadata.name: grafana}
    - namespaceSelector:
        matchLabels: {kubernetes.io/metadata.name: monitoring}
```

## Values

| Key | Default | |
|---|---|---|
| `config` | `{log: {format: json}, sources: {}}` | service configuration |
| `replicaCount` | `1` | more than one needs `config.redis` |
| `image.repository` | `docker.io/sakharovmaksim/grafana-infinity-datasource-cache` | |
| `image.tag` | chart `appVersion` | |
| `goMemLimit` | `200MiB` | `GOMEMLIMIT`, ~80% of `resources.limits.memory` |
| `extraEnv`, `extraEnvFrom`, `extraVolumes`, `extraVolumeMounts` | `[]` | secrets and files |
| `service.port` | `80` | keeps datasource URLs without a port |
| `resources` | requests `50m`/`64Mi`, limit `256Mi` | |
| `podDisruptionBudget` | enabled, `maxUnavailable: 1` | `minAvailable` wins when set |
| `serviceMonitor.enabled` | `false` | Prometheus Operator `ServiceMonitor` |
| `networkPolicy.enabled` | `false` | requires `networkPolicy.from` |

The rest — security contexts, probes, scheduling, annotations — is in [values.yaml](https://github.com/sakharovmaksim/grafana-infinity-datasource-cache/blob/master/charts/grafana-infinity-datasource-cache/values.yaml).
