# Testing TSIGMA Without Data

**Purpose**: Test admin interface and configuration setup before connecting to live traffic signals.

**Last Updated**: 2026-03-03

---

## Quick Start (Admin/Config Testing)

### 1. Setup Database (Empty)

```bash
# Create test database
createdb tsigma_test

# Install TimescaleDB (optional, but recommended)
psql tsigma_test -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"

# Run migrations (creates empty tables)
alembic upgrade head
```

**Result**: Empty database with all tables created

---

### 2. Configure Minimal Mode

```bash
# Copy minimal config
cp .env.example .env

# Edit if needed (Azure credentials, database password)
nano .env
```

**Key settings**:
```env
# All collection disabled
TSIGMA_ENABLE_COLLECTOR=false
TSIGMA_ENABLE_SCHEDULER=false
```

---

### 3. Start TSIGMA (No Data Collection)

```bash
# Start server
uvicorn tsigma.app:app --host 0.0.0.0 --port 8080 --reload
```

**Logs should show**:
```
INFO: database_initialized, db_type=postgresql
INFO: tsigma_started (collector and scheduler disabled)
```

**No background services running** - Just FastAPI with UI/API

---

### 4. Access Admin UI

```bash
open http://localhost:8080/login
```

**Available Pages** (work without data):

| Page | URL | Works Without Data? |
|------|-----|---------------------|
| **Login** | `/login` | ✅ Yes (test OAuth flow) |
| **Admin Dashboard** | `/admin` | ✅ Yes (Route not implemented — use `/admin/users` or `/admin/settings`) |
| **User Management** | `/admin/users` | ✅ Yes (CRUD users) |
| **API Keys** | `/admin/api_keys` | ✅ Yes (generate keys) |
| **Watchdog Config** | `/admin/watchdog` | ✅ Yes (Planned — not yet implemented) |
| **Signal Config** | `/signals` | ✅ Yes (add signals) |
| **Collection Config** | `/signals` | ✅ Yes (configure collection per signal) |
| **Health Check** | `/health` | ⚠️ Partial (JSON health check endpoint, not an HTML dashboard) |
| **Analytics** | `/api/v1/analytics/*` | ❌ No (API endpoints, not HTML pages — no data to return) |

---

## What You Can Test

### ✅ Authentication & Authorization

**Test OAuth flow** (if Azure configured):
1. Click "Sign in with Microsoft"
2. Authenticate with Azure Entra ID
3. Redirected back to TSIGMA
4. Session cookie created

**Test local login** (if enabled):
1. Enter username/password
2. Authenticate against database
3. Session created

**Test RBAC**:
1. Create user with "viewer" role
2. Try accessing `/admin/users` → Should get 403 Forbidden
3. Assign "admin" role
4. Try again → Should work

---

### ✅ Admin Interface

**User Management**:
1. Add user (email, name, role)
2. Assign roles (viewer, operator, admin)
3. Disable user
4. View audit log

**API Key Management**:
1. Generate API key (device, script, integration)
2. Set expiration date
3. Test key with `curl -H "Authorization: Bearer <token>"`
4. Revoke key

**Watchdog Configuration**:
1. Set scan intervals (detector: 15 min, splits: 60 min)
2. Configure alert thresholds (split failure >20%)
3. Add email recipients (critical → on-call, warning → team)
4. Save configuration

---

### ✅ Signal Configuration

**Add Signals**:
1. Click "+ Add Signal"
2. Enter signal ID (gdot-0142)
3. Enter name, location, controller type
4. Save

**Configure Approaches/Detectors**:
1. Select signal
2. Add approach (Northbound, phase 2)
3. Add detectors (channel 5, 6, 7)
4. Set detector distances from stop bar

---

### ✅ Collection Configuration

**Configure FTP polling per signal**:
1. Select a signal
2. Add collection config (protocol, username, password, decoder)
3. Collection config is stored in the signal's `metadata` JSONB column
4. The `CollectorService` picks up changes on the next poll cycle

---

### ⚠️ What WON'T Work (No Data)

**Analytics Charts**:
- PCD chart → Empty (no detector events)
- Volume chart → Empty (no volume data)
- Delay chart → Empty (no delay data)

**Expected behavior**: Charts show "No data available" message

**Health Check** (`/health`):
- Returns JSON health status, not an HTML dashboard
- Shows service component status

---

## Seed Test Data (Optional)

If you want to test UI with fake data:

```sql
-- Insert fake signal
INSERT INTO signal (signal_id, primary_street, secondary_street, enabled)
VALUES ('TEST-001', 'Test St', 'Main St', true);

-- Insert fake events (for chart testing)
INSERT INTO controller_event_log (signal_id, event_time, event_code, event_param)
VALUES
    ('TEST-001', NOW(), 82, 5),           -- Detector On, channel 5
    ('TEST-001', NOW() + INTERVAL '1 second', 81, 5),  -- Detector Off, channel 5
    ('TEST-001', NOW() + INTERVAL '5 seconds', 1, 2);  -- Phase Green, phase 2
```

**Now charts will render** with test data

---

## Testing Checklist

### Phase 1: Startup (No Data)

- [ ] `alembic upgrade head` succeeds (migrations run)
- [ ] `uvicorn tsigma.app:app` starts without errors
- [ ] No background services start (logs show "disabled")
- [ ] Health check responds: `curl http://localhost:8080/health`

### Phase 2: Authentication

- [ ] Login page renders (`/login`)
- [ ] OAuth flow works (Azure Entra ID or Google)
- [ ] Local login works (username/password)
- [ ] Session cookie created
- [ ] Logout works

### Phase 3: Admin Interface

- [ ] Admin dashboard accessible (`/admin/users` or `/admin/settings`)
- [ ] User management works (add, edit roles, disable)
- [ ] API key generation works
- [ ] Watchdog config page loads
- [ ] Settings can be changed and saved

### Phase 4: Configuration

- [ ] Signal config page loads (`/signals`)
- [ ] Can add signal (stored in database)
- [ ] Collection config works (stored in signal metadata)
- [ ] Configuration changes take effect on next poll cycle

### Phase 5: UI (With Mock Data)

- [ ] Dashboard renders (empty charts OK)
- [ ] Navigation works (all pages accessible)
- [ ] Page fragments load correctly (check browser network tab)
- [ ] Alpine.js state works (date pickers, dropdowns)

---

## Expected Result

**After 1-2 hours of testing**:
- ✅ Admin interface fully functional
- ✅ Configuration system working
- ✅ Authentication working (OAuth + local)
- ✅ Ready to connect to real GDOT data

**Then**: Enable collection services, connect to real traffic signals

---

**You can absolutely test admin/config before connecting to real GDOT network!**