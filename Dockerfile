# syntax=docker/dockerfile:1.7
#
# Build args:
#   DB_EXTRAS  — pip extras for the database driver(s) to install.
#                Default: postgresql only.
#                Examples:
#                  --build-arg DB_EXTRAS=                  # PostgreSQL only (default)
#                  --build-arg DB_EXTRAS=mssql             # + MS-SQL (aioodbc)
#                  --build-arg DB_EXTRAS=oracle            # + Oracle (oracledb)
#                  --build-arg DB_EXTRAS=mysql             # + MySQL (aiomysql)
#                  --build-arg DB_EXTRAS=mssql,oracle      # multiple
#
# Built variants typically tagged: tsigma:<version>, tsigma:<version>-mssql, etc.

# ============================================================================
# Stage 1 — builder
# Installs build deps and compiles wheels into a venv.
# ============================================================================
FROM python:3.14-slim AS builder

ARG DB_EXTRAS=""

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH

# Build deps for native wheels (cryptography, grpcio, asyncpg, aioodbc, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libpq-dev \
        libffi-dev \
        libssl-dev \
        unixodbc-dev \
        && rm -rf /var/lib/apt/lists/*

RUN python -m venv "$VIRTUAL_ENV"

WORKDIR /build

# Install dependencies first (cached as long as pyproject doesn't change)
COPY pyproject.toml ./
RUN pip install --upgrade pip setuptools wheel \
    && if [ -n "$DB_EXTRAS" ]; then \
           pip install -e ".[${DB_EXTRAS}]" ; \
       else \
           pip install -e . ; \
       fi

# Copy the source and install (editable install picks up the src dir)
COPY tsigma ./tsigma
COPY alembic.ini ./
COPY alembic ./alembic

# Re-run pip install to register the package metadata against the source tree
RUN pip install --no-deps -e .


# ============================================================================
# Stage 2 — runtime
# Slim image with just the venv and runtime deps. Non-root user.
# Per-DB system libraries:
#   - PostgreSQL: libpq5 (always installed)
#   - MS-SQL: unixodbc + Microsoft msodbcsql18 (installed when DB_EXTRAS contains "mssql")
#   - Oracle: oracledb thin mode is pure Python (no Instant Client needed)
#   - MySQL: aiomysql is pure Python (no MySQL client lib needed)
# ============================================================================
FROM python:3.14-slim AS runtime

ARG DB_EXTRAS=""

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    TSIGMA_API_HOST=0.0.0.0 \
    TSIGMA_API_PORT=8080

# Base runtime libs (PostgreSQL + ODBC core), with MS-SQL ODBC driver added
# when DB_EXTRAS includes "mssql". Single RUN to keep the layer tidy.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libpq5 \
        unixodbc \
        ca-certificates \
        gnupg \
        curl \
        tini && \
    if echo ",${DB_EXTRAS}," | grep -q ",mssql,"; then \
        curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
            | gpg --dearmor -o /usr/share/keyrings/microsoft-archive-keyring.gpg && \
        DEBIAN_VERSION=$(. /etc/os-release && echo "$VERSION_ID") && \
        echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-archive-keyring.gpg] https://packages.microsoft.com/debian/${DEBIAN_VERSION%%.*}/prod $(. /etc/os-release && echo "$VERSION_CODENAME") main" \
            > /etc/apt/sources.list.d/mssql-release.list && \
        apt-get update && \
        ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 ; \
    fi && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --system --gid 10001 tsigma && \
    useradd --system --uid 10001 --gid tsigma --home /app --shell /usr/sbin/nologin tsigma

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /build/tsigma ./tsigma
COPY --from=builder /build/alembic.ini ./alembic.ini
COPY --from=builder /build/alembic ./alembic

# Cold-storage path (only used when TSIGMA_STORAGE_COLD_ENABLED=true)
RUN mkdir -p /var/lib/tsigma/cold && chown -R tsigma:tsigma /var/lib/tsigma /app

USER tsigma

EXPOSE 8080

# tini reaps zombie procs cleanly when running as PID 1
ENTRYPOINT ["/usr/bin/tini", "--"]

# Default: run via uvicorn so external orchestrators control replicas/scaling.
# Override with `python -m tsigma.main` if you want the in-process scheduler.
CMD ["uvicorn", "tsigma.app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=3).status==200 else 1)"
