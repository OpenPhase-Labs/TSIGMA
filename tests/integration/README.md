# Integration tests

Every test under `tests/integration/` is parametrised over the four
supported dialects (PostgreSQL, MS-SQL, Oracle, MySQL).  Each dialect
is discovered and skipped independently, so running the suite on a
laptop with only Docker available is a legitimate workflow.

## Running

```
pytest -m integration
```

Collect-only to sanity-check that everything imports and parametrises
without actually hitting a database:

```
pytest -m integration --collect-only
```

## Configuring backends

Tests look for a DB backend in this order, per dialect:

1. An environment variable giving a SQLAlchemy URL (preferred on CI
   and for developers running a persistent DB locally).
2. A testcontainers-managed Docker container spun up on demand and
   shared across the pytest session.
3. If neither is available, tests for **that dialect only** skip — the
   other three continue to run.

### Environment variables

| Dialect     | Variable                                             |
|-------------|------------------------------------------------------|
| PostgreSQL  | `TSIGMA_TEST_PG_URL` (or legacy `TSIGMA_TEST_DB_URL`)|
| MS-SQL      | `TSIGMA_TEST_MSSQL_URL`                              |
| Oracle      | `TSIGMA_TEST_ORACLE_URL`                             |
| MySQL       | `TSIGMA_TEST_MYSQL_URL`                              |

`TSIGMA_TEST_DB_URL` is kept as an alias for `TSIGMA_TEST_PG_URL` so
pre-multi-dialect scripts and CI pipelines continue to work.

Example URL shapes (sync scheme — the conftest rewrites to the async
driver at runtime):

```
postgresql+psycopg2://tsigma:tsigma@localhost:5432/tsigma_test
mssql+pyodbc://sa:TsigmaTest1!@localhost:1433/master?driver=ODBC+Driver+18+for+SQL+Server
oracle+oracledb://system:oracle@localhost:1521/FREEPDB1
mysql+pymysql://tsigma:tsigma@localhost:3306/tsigma_test
```

### Testcontainers fallback

If an environment variable is not set, the fixtures fall back to
`testcontainers` and pull the image on first use.  This requires:

* Docker Desktop (or an equivalent Docker daemon) running and
  reachable at the default socket.
* The `integration` extras installed:
  `pip install -e '.[integration]'`.

Approximate image sizes on first pull:

| Image                                     | Size     |
|-------------------------------------------|----------|
| `timescale/timescaledb:latest-pg16`       | ~700 MB  |
| `mcr.microsoft.com/mssql/server:2022-latest` | ~1.4 GB |
| `gvenzl/oracle-free:23-slim-faststart`    | ~2 GB    |
| `mysql:8`                                 | ~600 MB  |

Containers are session-scoped — the spin-up cost is paid once per
`pytest` invocation and amortised across every test.

## Skip semantics

Each dialect's discovery runs independently.  If you only have
PostgreSQL configured (or only Docker is available for PostgreSQL),
the PG tests run and the MS-SQL / Oracle / MySQL parametrisations
skip with a clear reason:

```
SKIPPED [reason=mssql: no DB available (env vars ('TSIGMA_TEST_MSSQL_URL',) unset, testcontainers/Docker unavailable)]
```

Running the suite on a workstation with no env vars and no Docker
produces `N skipped` with no failures — that is the designed "nothing
is configured" outcome, not a broken run.
