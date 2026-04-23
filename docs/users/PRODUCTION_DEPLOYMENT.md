# TSIGMA Production Deployment Guide

**Purpose**: Deploy TSIGMA in production with no external dependencies.

**Last Updated**: 2026-03-03

---

## Critical Production Requirements

### ✅ No CDN Dependencies

**Why**: Security, reliability, compliance, air-gapped networks

**Solution**: Self-host all frontend assets

```bash
# Download assets before deployment
./scripts/download_vendor_libs.sh

# Verify no external dependencies
grep -r "cdn\|unpkg\|jsdelivr" tsigma/templates/
# Should return: No matches (all assets self-hosted)
```

**Assets bundled**:
- `/static/css/tailwind.css` - Built from source (not CDN)
- `/static/js/vendor/htmx.min.js` - Downloaded, verified hash (Not currently used)
- `/static/js/vendor/alpine.min.js` - Downloaded, verified hash
- `/static/js/vendor/echarts.min.js` - Downloaded, verified hash

---

## Deployment Options

### Option 1: Docker (Recommended)

**Build image**:
```bash
docker build -t tsigma:latest .
```

**Run**:
```bash
docker run -d \
  -p 8080:8080 \
  -e TSIGMA_DB_TYPE=postgresql \
  -e TSIGMA_PG_HOST=postgres \
  -e TSIGMA_PG_DATABASE=tsigma \
  -e TSIGMA_AUTH_MODE=oidc \
  -e TSIGMA_OIDC_TENANT_ID=gdot-tenant \
  --name tsigma \
  tsigma:latest
```

**All assets bundled in image** - No external network access required

---

### Option 2: Kubernetes

```yaml
# tsigma-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tsigma
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: tsigma
        image: tsigma:latest
        ports:
        - containerPort: 8080
        env:
        - name: TSIGMA_DB_TYPE
          value: "postgresql"
        # ... (other env vars from ConfigMap/Secret)
```

---

### Option 3: Bare Metal / VM

```bash
# 1. Download assets
./scripts/download_vendor_libs.sh

# 2. Install dependencies
pip install -e .

# 3. Build Tailwind
# Note: The current deployment uses the Tailwind CDN play build
# (/static/vendor/tailwind/tailwind.min.js), not a compiled CSS file.
npx tailwindcss -i tsigma/static/css/tailwind.src.css \
                 -o tsigma/static/css/tailwind.css \
                 --minify

# 4. Run migrations
alembic upgrade head

# 5. Start with systemd
sudo systemctl start tsigma
```

---

## Asset Verification (Security)

### Subresource Integrity (Optional)

> **Note**: HTMX is not currently used in the application.

If CDN fallback is needed for development:

```html
<script src="/static/js/vendor/htmx.min.js"
        integrity="sha384-hash-here"
        crossorigin="anonymous"></script>

<!-- CDN fallback (only if self-hosted fails) -->
<script>
if (!window.htmx) {
    document.write('<script src="https://unpkg.com/htmx.org@1.9.10" integrity="sha384-..." crossorigin="anonymous"><\/script>');
}
</script>
```

**Recommendation**: Don't use CDN fallback in production (air-gapped networks can't access CDN anyway)

---

## Build Process

### 1. Download Assets

```bash
./scripts/download_vendor_libs.sh
```

**Downloads**:
- HTMX 1.9.10 (~15 KB) (Not currently used)
- Alpine.js 3.14.9 (~20 KB)
- ECharts 5.6.0 (~1 MB)

**Verifies SHA256 hashes** (prevent supply chain attacks)

---

### 2. Build TailwindCSS

Note: The current deployment uses the Tailwind CDN play build (`/static/vendor/tailwind/tailwind.min.js`), not a compiled CSS file.

```bash
# Install Tailwind CLI (one-time)
npm install -D tailwindcss

# Build CSS (scans templates for used classes)
npx tailwindcss -o tsigma/static/css/tailwind.css --minify
```

**Result**: ~30 KB minified CSS (only classes actually used in templates)

**Alternative** (no npm):
```bash
# Download standalone Tailwind CLI
curl -sLO https://github.com/tailwindlabs/tailwindcss/releases/download/v3.4.1/tailwindcss-linux-x64
chmod +x tailwindcss-linux-x64
./tailwindcss-linux-x64 -o tsigma/static/css/tailwind.css --minify
```

---

### 3. Configure Static Files in FastAPI

```python
# tsigma/app.py

from fastapi.staticfiles import StaticFiles

app = FastAPI(...)

# Mount static files
app.mount("/static", StaticFiles(directory="tsigma/static"), name="static")
```

**Serves**:
- `/static/css/tailwind.css`
- `/static/js/vendor/*.js`

---

## Production Checklist

### Security

- ✅ No CDN dependencies (all assets self-hosted)
- ✅ Asset integrity verification (SHA256 hashes)
- ✅ HTTPS only (set `secure=True` on cookies)
- ✅ HSTS preload (optional): set `TSIGMA_HSTS_PRELOAD=true` and submit domain at [hstspreload.org](https://hstspreload.org/)
- ✅ Environment secrets (never commit .env)
- ✅ Database credentials encrypted
- ✅ SMTP password encrypted

### Performance

- ✅ Static files cached (browser cache, CDN if using reverse proxy)
- ✅ Gzip compression (nginx reverse proxy)
- ✅ Asset minification (TailwindCSS minified, JS minified)
- ✅ Database connection pooling
- ✅ TimescaleDB continuous aggregates

### Reliability

- ✅ Health check endpoint (`/health` and `/ready`)
- ✅ Graceful shutdown (wait for tasks to complete)
- ✅ Database migration on startup (idempotent)
- ✅ Log aggregation (structlog JSON output)

---

## Air-Gapped Deployment

For networks with **no internet access**:

1. **Build Docker image on internet-connected machine**:
   ```bash
   docker build -t tsigma:latest .
   docker save tsigma:latest -o tsigma-image.tar
   ```

2. **Transfer image to air-gapped network**:
   ```bash
   scp tsigma-image.tar production-server:/tmp/
   ```

3. **Load and run**:
   ```bash
   docker load -i /tmp/tsigma-image.tar
   docker run -d tsigma:latest
   ```

**All assets included in image** - No external network required

---

## Asset Update Process

When updating frontend libraries:

1. Update version in `scripts/download_vendor_libs.sh`
2. Run `./scripts/download_vendor_libs.sh`
3. Verify new assets work locally
4. Rebuild Docker image
5. Deploy to production

**Controlled updates** - Not automatic CDN changes

---

**Document Version**: 1.0
**Last Updated**: 2026-03-03
**Owner**: OpenPhase Labs
