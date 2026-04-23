# TSIGMA Helm Chart

Deploys [TSIGMA](https://github.com/OpenPhase-Labs/TSIGMA) on Kubernetes — REST/GraphQL API, collector, scheduler, and gRPC ingestion server in one chart.

## Prerequisites

- Kubernetes 1.27+
- Helm 3.12+
- An existing PostgreSQL 18+ instance, **separate from this chart**. PostgreSQL is intentionally not bundled — production deployments will typically use:
  - A managed service (AWS RDS, Cloud SQL, Aiven, Crunchy Bridge, Timescale Cloud), or
  - An in-cluster operator (CrunchyData PGO, Zalando postgres-operator, CloudNativePG), or
  - An HA cluster you already run.

  Point `database.host` at the cluster's primary read-write endpoint (or whatever Service/DNS name routes to the current primary in a failover-aware way). TSIGMA does not currently route reads to replicas — all queries go to the primary.

### Required PostgreSQL extensions
- `pgcrypto` — provides `gen_random_uuid()`. Pre-installed on every managed service we've checked.
- `timescaledb` (recommended) — enables hypertables on `controller_event_log` and continuous aggregates for cycle data. Without it, the migration falls back to regular tables and the scheduler runs the aggregate jobs itself.

The Alembic migration enables/uses these extensions if available and silently no-ops if they aren't — so the chart works either way, you just lose the TimescaleDB performance benefits.

### TLS to the database
Most managed services require TLS. Set `database.ssl.enabled=true` and pick the `sslMode` your provider needs (`require` is enough for most; `verify-full` is recommended for production and needs the CA bundle):

```yaml
database:
  ssl:
    enabled: true
    sslMode: verify-full
    caCertSecret: tsigma-db-ca   # Secret with key `ca.crt`
```

The CA cert is mounted at `/etc/tsigma/db-tls/ca.crt` and `PGSSLROOTCERT` is set automatically.

## Install

```bash
# Quick start (sets a database password — replace with your real password)
helm install tsigma ./deploy/helm/tsigma \
  --namespace tsigma --create-namespace \
  --set database.host=postgres.example.com \
  --set database.password='REPLACE_ME' \
  --set auth.local.adminPassword='REPLACE_ME'

# Or with a values file
helm install tsigma ./deploy/helm/tsigma -n tsigma -f my-values.yaml
```

The pre-install hook runs `alembic upgrade head` against the configured database before the Deployment rolls out.

## Upgrade

```bash
helm upgrade tsigma ./deploy/helm/tsigma -n tsigma -f my-values.yaml
```

Migrations run again as a `pre-upgrade` hook before the new pods start.

## Configuration

See [`values.yaml`](values.yaml) for the full annotated reference. Common overrides:

| Setting | Default | Notes |
|---------|---------|-------|
| `image.repository` | `ghcr.io/openphase-labs/tsigma` | |
| `image.tag` | `""` (uses `Chart.appVersion`) | Override per release |
| `replicaCount` | `1` | Use `autoscaling.enabled=true` for HPA |
| `database.type` | `postgresql` | `postgresql` / `mssql` / `oracle` / `mysql` |
| `database.host` | `tsigma-postgres` | Hostname of your existing DB |
| `database.password` | (none) | Required unless `existingSecret` is set |
| `database.existingSecret` | `""` | Secret with key `TSIGMA_PG_PASSWORD` |
| `auth.mode` | `local` | `local` / `oidc` / `oauth2` |
| `auth.local.adminPassword` | `changeme` | **Change immediately** |
| `grpcIngestion.enabled` | `true` | Exposes port 50051 |
| `grpcIngestion.tls.enabled` | `false` | Set true + `secretName` for TLS-protected gRPC |
| `ingress.enabled` | `false` | Standard nginx-style Ingress for the REST API |
| `storage.cold.enabled` | `false` | Enables Parquet cold-archive volume |

### Using existing secrets

To avoid putting secrets in values files, create the Secret out-of-band and reference it:

```bash
kubectl -n tsigma create secret generic tsigma-db-secret \
  --from-literal=TSIGMA_PG_PASSWORD='your-real-password'
```

```yaml
# values.yaml
database:
  existingSecret: tsigma-db-secret
auth:
  mode: oidc
  oidc:
    tenantId: 11111111-1111-1111-1111-111111111111
    clientId: 22222222-2222-2222-2222-222222222222
    existingSecret: tsigma-oidc-secret    # must contain TSIGMA_OIDC_CLIENT_SECRET
    redirectUri: https://tsigma.example.com/auth/oidc/callback
```

### Exposing gRPC ingestion to external devices

Field devices typically connect to TSIGMA from outside the cluster. Two options:

1. **LoadBalancer** — fastest, single port:
   ```yaml
   grpcIngestion:
     service:
       type: LoadBalancer
       port: 50051
     tls:
       enabled: true
       secretName: tsigma-grpc-tls   # contains tls.crt and tls.key
   ```

2. **Ingress with HTTP/2 + TLS** — controller-dependent (nginx-ingress and Contour both support it; check your controller's gRPC docs).

## Uninstall

```bash
helm uninstall tsigma -n tsigma
```

The Alembic schema is **not** dropped — that's intentional. Drop the database manually if you want a clean slate.

## Customization

The Helm chart is intended to cover the common deployment shape. For deeper customization (sidecars, custom volumes, NetworkPolicy, etc.), copy the chart under your own GitOps repo and modify the templates directly — there's no shame in vendoring.
