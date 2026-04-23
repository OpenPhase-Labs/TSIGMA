# FTP/SFTP Polling Configuration Guide

**Purpose**: Configure TSIGMA to poll legacy traffic controllers via FTP/SFTP for event log files.

**Last Updated**: 2026-04-22

---

## Overview

TSIGMA polls traffic controllers via FTP/SFTP to retrieve event log files periodically for ingestion and analysis.

---

## Configuration Sources

TSIGMA uses a three-tier configuration system. See [ARCHITECTURE.md - Configuration Priority](ARCHITECTURE.md#configuration-priority) for the complete priority order and rationale.

**Summary**: Environment Variables > YAML File > Database

### When to Use Each Source

- **Environment Variables**: Deployment-specific overrides (secrets, hosts, ports)
- **YAML File**: Version-controlled configuration (development, testing)
- **Database**: Shared baseline configuration (production hot-reload without restart)

---

## Configuration Method

### Database Configuration (signal.metadata JSONB)

Collection config is stored in the `signal.metadata` JSONB column under the `"collection"` key. The `CollectorService` reads this at poll time.

```sql
-- Add FTP collection config to a signal
UPDATE signal
SET metadata = jsonb_set(
    COALESCE(metadata, '{}'),
    '{collection}',
    '{
        "method": "ftp_pull",
        "protocol": "sftp",
        "host": "192.168.1.100",
        "username": "atspm",
        "password": "secret123",
        "remote_dir": "/data/logs",
        "decoder": "asc3"
    }'
)
WHERE signal_id = 'GDOT-0142';
```

`host` is required in the collection config JSONB. Example: `"host": "192.168.1.100"`

**Hot-Reload**: The `CollectorService` queries the `signal` table each poll cycle, so config changes take effect on the next cycle (default: every 300 seconds)

---

### Environment Variables (Collector Settings)

Global collector settings are configured via environment variables. Per-signal collection config is always in the database.

```env
# .env
TSIGMA_ENABLE_COLLECTOR=true
TSIGMA_COLLECTOR_POLL_INTERVAL=300        # seconds between poll cycles
TSIGMA_COLLECTOR_MAX_CONCURRENT=50        # max simultaneous FTP connections
TSIGMA_CHECKPOINT_FUTURE_TOLERANCE_SECONDS=300
TSIGMA_CHECKPOINT_SILENT_CYCLES_THRESHOLD=3
```

---

## Collection Config Fields

These fields go in `signal.metadata->'collection'` JSONB.

### Required Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `method` | string | Must be `"ftp_pull"` | `"ftp_pull"` |
| `protocol` | string | `"ftp"`, `"ftps"`, or `"sftp"` | `"sftp"` |
| `host` | string | FTP/SFTP host address | `"192.168.1.100"` |
| `username` | string | Authentication username | `"atspm"` |
| `remote_dir` | string | Remote directory path | `"/data/logs"` |

### Optional Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `port` | integer | `21` (FTP), `990` (FTPS), `22` (SFTP) | Server port |
| `password` | string | `""` | Password (or use `ssh_key_path` for SFTP) |
| `ssh_key_path` | string | `null` | SSH private key path (SFTP only) |
| `file_extensions` | array | `[".dat", ".csv", ".log"]` | File extensions to download |
| `decoder` | string | auto-detect | Decoder type |
| `mode` | string | `"passive"` | `"passive"` (non-destructive) or `"rotate"` (SNMP) |
| `passive_mode` | boolean | `true` | Use FTP passive mode (FTP/FTPS only) |
| `snmp_version` | string | `"v1"` | SNMP protocol version: `"v1"`, `"v2c"`, or `"v3"` |
| `snmp_community` | string | `"public"` | SNMP v1/v2c community string (rotate mode) |
| `snmp_port` | integer | `161` | SNMP port (rotate mode) |
| `snmp_username` | string | `""` | SNMPv3 USM username |
| `snmp_security_level` | string | `"authPriv"` | SNMPv3: `"noAuthNoPriv"`, `"authNoPriv"`, `"authPriv"` |
| `snmp_auth_protocol` | string | `"SHA"` | SNMPv3 auth: `"MD5"`, `"SHA"`, `"SHA256"`, `"SHA384"`, `"SHA512"` |
| `snmp_auth_passphrase` | string | `""` | SNMPv3 auth passphrase (encrypted at rest) |
| `snmp_priv_protocol` | string | `"AES128"` | SNMPv3 privacy: `"DES"`, `"AES128"`, `"AES192"`, `"AES256"` |
| `snmp_priv_passphrase` | string | `""` | SNMPv3 privacy passphrase (encrypted at rest) |
| `rotate_filename` | string | `null` | Specific file to rotate (rotate mode) |

### SNMPv3 Rotate Mode Example

For controllers supporting SNMPv3 with authentication and encryption:

```json
{
  "method": "ftp_pull",
  "protocol": "sftp",
  "host": "192.168.1.100",
  "username": "tsigma",
  "password": "controller-password",
  "remote_dir": "/data/logs",
  "mode": "rotate",
  "snmp_version": "v3",
  "snmp_username": "tsigma-snmp",
  "snmp_security_level": "authPriv",
  "snmp_auth_protocol": "SHA256",
  "snmp_auth_passphrase": "my-auth-passphrase",
  "snmp_priv_protocol": "AES128",
  "snmp_priv_passphrase": "my-privacy-passphrase",
  "rotate_filename": "ATSPM.dat"
}
```

**Notes:**
- `snmp_auth_passphrase` and `snmp_priv_passphrase` are encrypted at rest if
  encryption is configured (see SECURITY.md)
- For controllers that only support SNMP v1, omit the `snmp_version` field
  (defaults to `"v1"`) or set it explicitly
- `snmp_community` is ignored when `snmp_version` is `"v3"`

---

## Decoder Types

| Decoder | Vendor | Format | File Extension |
|---------|--------|--------|----------------|
| `auto` | Auto-detect | Multiple | Any |
| `asc3` | Econolite ASC/3 | Binary (10-byte records) | `.dat` |
| `siemens` | Siemens | SEPAC text | `.txt` |
| `peek` | Peek/McCain | ATC binary | `.dat` |
| `maxtime` | MaxTime/Intelight | XML/binary | `.xml`, `.dat` |
| `csv` | Generic | CSV | `.csv` |

**Recommendation**: Use `"auto"` for mixed deployments (TSIGMA auto-detects format)

---

## Polling Checkpoint (Non-Destructive State Tracking)

TSIGMA never deletes files from the controller. Instead, it tracks what has already been ingested via a persistent `polling_checkpoint` table in the database.

### How It Differs from ATSPM

| | ATSPM 4.x | ATSPM 5.x | TSIGMA |
|---|-----------|-----------|--------|
| **State storage** | None (file deletion = state) | In-memory set | Database table |
| **Survives restart** | N/A (files gone) | No | Yes |
| **Destructive** | Yes (deletes files) | No | No |
| **Multiple consumers** | No (first poller wins) | No (no coordination) | Yes (independent checkpoints) |

### What Gets Tracked Per Signal

| Field | Purpose |
|-------|---------|
| `last_filename` | Most recently ingested file |
| `last_file_mtime` | Modification time of that file on the remote server |
| `files_hash` | SHA-256 of sorted filenames — quick "anything new?" check |
| `last_successful_poll` | When the last poll completed (for health monitoring) |
| `consecutive_errors` | Failure count — enables backoff and alerting |

### Poll Cycle Behavior

```
Connect to controller FTP
    │
    ├─ List files in remote_path matching file_pattern
    │
    ├─ Compute hash of filenames
    │   └─ If hash == checkpoint.files_hash → no new files, disconnect
    │
    ├─ Filter: keep only files where mtime > checkpoint.last_file_mtime
    │
    ├─ Download new files → decode → ingest
    │
    ├─ On success:
    │   └─ UPDATE polling_checkpoint SET
    │        last_filename = <newest file>,
    │        last_file_mtime = <newest mtime>,
    │        files_hash = <new hash>,
    │        last_successful_poll = NOW(),
    │        consecutive_errors = 0
    │
    └─ On failure:
        └─ UPDATE polling_checkpoint SET
             consecutive_errors = consecutive_errors + 1,
             last_error = <error message>,
             last_error_time = NOW()
             -- Checkpoint fields NOT updated — next poll retries from same point
```

### Resetting a Checkpoint

If a controller is replaced or you need to re-ingest all data:

```sql
-- Reset checkpoint for a single signal (re-polls everything on next cycle)
DELETE FROM polling_checkpoint
WHERE signal_id = 'GDOT-0142' AND method = 'ftp_pull';

-- Reset all FTP checkpoints (re-polls everything)
DELETE FROM polling_checkpoint WHERE method = 'ftp_pull';
```

### Schema Reference

See [DATABASE_SCHEMA.md — Polling Checkpoint Table](DATABASE_SCHEMA.md#polling-checkpoint-table) for the full table definition.

---

## Polling Intervals

### Recommended Intervals by Use Case

| Use Case | Interval | Rationale |
|----------|----------|-----------|
| **Standard ATSPM** | 3600s (1 hour) | Industry standard, balances freshness vs load |
| **Critical Intersection** | 900s (15 min) | Higher frequency for important routes |
| **Low-Traffic Rural** | 7200s (2 hours) | Reduce load for minimal activity |
| **Development/Testing** | 300s (5 min) | Faster feedback during development |

**Note**: Controllers typically flush logs hourly, so polling more frequently than 1 hour provides diminishing returns.

---

## Security

### SSH Keys (SFTP - Recommended)

**Generate SSH key pair**:
```bash
ssh-keygen -t rsa -b 4096 -f /etc/tsigma/keys/controller_rsa -N ""
```

**Copy public key to controller**:
```bash
ssh-copy-id -i /etc/tsigma/keys/controller_rsa.pub atspm@192.168.1.100
```

**Configuration**:
```sql
UPDATE signal
SET metadata = jsonb_set(
    COALESCE(metadata, '{}'),
    '{collection}',
    '{
        "method": "ftp_pull",
        "protocol": "sftp",
        "host": "192.168.1.100",
        "username": "atspm",
        "ssh_key_path": "/etc/tsigma/keys/controller_rsa",
        "remote_dir": "/data/logs",
        "decoder": "asc3"
    }'
)
WHERE signal_id = 'GDOT-0142';
-- No password field needed when using SSH key authentication
```

### Password Storage (Database)

Credentials in `signal_metadata["collection"]` are encrypted at rest using Fernet symmetric encryption. Passwords and SSH key paths are automatically encrypted when a signal is created or updated via the API, and decrypted at poll time by the `CollectorService`.

**Setup:**

1. Generate a Fernet key:
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

2. Configure the key (choose one):
   ```env
   # Option 1: Environment variable
   TSIGMA_SECRET_KEY=your-fernet-key-here

   # Option 2: File
   TSIGMA_SECRET_KEY_FILE=/run/secrets/tsigma_key

   # Option 3: HashiCorp Vault
   TSIGMA_SECRET_KEY_VAULT_URL=https://vault.example.com
   TSIGMA_SECRET_KEY_VAULT_PATH=secret/data/tsigma
   ```

**Important:**
- Back up your encryption key. Lost key = lost credentials (re-enter on all signals).
- Rotate keys by decrypting with old key, then re-encrypting with new key.
- SSH key authentication (SFTP) is preferred over passwords when controllers support it.

---

## Organizing Signals with Metadata

Use the `signal.metadata` JSONB column to tag signals by district, region, project, or any custom attribute.

### Metadata Examples

```sql
-- Tag signals by district and project
UPDATE signal
SET metadata = jsonb_set(
    COALESCE(metadata, '{}'),
    '{tags}',
    '{"district": "north", "region": "downtown", "project": "2024-modernization"}'
)
WHERE signal_id IN ('GDOT-0142', 'GDOT-0143');
```

### Filter by Metadata

```sql
-- Get all signals in north district
SELECT signal_id, ip_address, metadata
FROM signal
WHERE metadata->'tags'->>'district' = 'north';

-- Get all signals for a project
SELECT signal_id, ip_address, metadata
FROM signal
WHERE metadata->'tags'->>'project' = '2024-modernization';
```

---

## Dynamic Configuration (Hot-Reload)

### Add Collection Config Without Restart

```sql
-- Add FTP collection config to an existing signal
UPDATE signal
SET metadata = jsonb_set(
    COALESCE(metadata, '{}'),
    '{collection}',
    '{
        "method": "ftp_pull",
        "protocol": "sftp",
        "host": "192.168.1.200",
        "username": "atspm",
        "password": "secret",
        "remote_dir": "/data/logs",
        "decoder": "asc3"
    }'
)
WHERE signal_id = 'GDOT-NEW-001';
```

Changes take effect on the next poll cycle (default: every 300 seconds).

### Disable Signal (Maintenance)

```sql
-- Disable polling for signal under maintenance
UPDATE signal SET enabled = false WHERE signal_id = 'GDOT-0142';

-- Re-enable after maintenance
UPDATE signal SET enabled = true WHERE signal_id = 'GDOT-0142';
```

**Effect**: Changes applied on next poll cycle

---

## Usage

The `FTPPullMethod` is a registered ingestion plugin. The `CollectorService` discovers it automatically and calls `poll_once()` for each enabled signal whose `signal_metadata->collection->method` is `"ftp_pull"`.

**Start the collector** by setting environment variables:

```env
TSIGMA_ENABLE_COLLECTOR=true
TSIGMA_COLLECTOR_POLL_INTERVAL=300    # seconds between poll cycles
TSIGMA_COLLECTOR_MAX_CONCURRENT=50    # max simultaneous FTP connections
```

**Configure signals** in the database:

```sql
UPDATE signal
SET metadata = jsonb_set(
    COALESCE(metadata, '{}'),
    '{collection}',
    '{
        "method": "ftp_pull",
        "protocol": "sftp",
        "host": "192.168.1.100",
        "username": "atspm",
        "password": "secret",
        "remote_dir": "/data/logs",
        "decoder": "asc3"
    }'
)
WHERE signal_id = 'GDOT-0142';
```

---

## Monitoring

### Health Check

```bash
curl http://localhost:8080/health
curl http://localhost:8080/ready
```

### Per-Signal Collection Status

Query the `polling_checkpoint` table for per-signal status:

```sql
SELECT signal_id, method, last_successful_poll,
       events_ingested, files_ingested,
       consecutive_errors, last_error
FROM polling_checkpoint
WHERE signal_id = 'GDOT-0142';
```

**Signals with errors**:
```sql
SELECT signal_id, method, consecutive_errors, last_error, last_error_time
FROM polling_checkpoint
WHERE consecutive_errors > 0
ORDER BY consecutive_errors DESC;

```

---

## Performance Tuning

### Parallel Polling (Python 3.14+ Free-Threaded)

With GIL removed, TSIGMA can poll multiple controllers in true parallel. The `CollectorService` uses an `asyncio.Semaphore` to bound concurrent connections:

```env
# Control max simultaneous FTP connections
TSIGMA_COLLECTOR_MAX_CONCURRENT=200
```

**Performance**: 150 signals × 1 hour interval = ~24 seconds per signal (with parallelism)

### Batch Processing

```python
# Increase batch size for high-volume files
# (trade memory for throughput)
decoder = ASC3Decoder()
events = await decoder.decode(large_file_data)  # Processes entire file in memory
```

---

## Troubleshooting

### Signal Not Polling

**Check configuration**:
```bash
# Verify signal is enabled and has collection config
curl http://localhost:8080/api/v1/signals/GDOT-0142
```

**Check logs**:
```bash
# Look for polling errors
tail -f /var/log/tsigma/collection.log | grep gdot-0142
```

### Authentication Failures

**SFTP**: Verify SSH key permissions
```bash
chmod 600 /etc/tsigma/keys/controller_rsa
ssh -i /etc/tsigma/keys/controller_rsa atspm@192.168.1.100
```

**FTP**: Test with command-line client
```bash
ftp 192.168.1.100
# username: atspm
# password: ***
```

### File Format Not Recognized

**Symptom**: `Unknown event log format` error

**Solution**: Inspect file header
```bash
hexdump -C /tmp/tsigma/ftp_cache/gdot-0142/events.dat | head
```

**Manually specify decoder** in the collection config JSONB:
```json
"decoder": "asc3"
```

---

## Best Practices

### Polling Intervals

✅ **Do**:
- Use 3600s (1 hour) for standard ATSPM
- Use 1800s (30 min) for critical signals
- Use 7200s (2 hours) for low-traffic rural signals

❌ **Don't**:
- Poll faster than controller flushes logs (typically hourly)
- Use intervals <300s (creates unnecessary load)

### File Patterns

✅ **Do**:
- Use specific patterns: `"events_*.dat"`, `"log_2026*.txt"`
- Exclude non-event files: `"!*.tmp"`, `"!*.bak"`

❌ **Don't**:
- Use `"*"` (downloads all files, including non-event data)
- Use too-restrictive patterns that miss new file formats

### Credentials

✅ **Do**:
- Use SSH keys for SFTP (no password in config)
- Store encrypted passwords in database
- Rotate credentials regularly

❌ **Don't**:
- Commit passwords to version control (YAML files)
- Use same password for all controllers
- Use default vendor passwords

---

## Configuration Schema

### Complete Example (signal_metadata JSONB)

```sql
-- Full collection config example
UPDATE signal
SET metadata = jsonb_set(
    COALESCE(metadata, '{}'),
    '{collection}',
    '{
        "method": "ftp_pull",
        "protocol": "sftp",
        "host": "192.168.1.100",
        "port": 22,
        "username": "atspm",
        "ssh_key_path": "/etc/tsigma/keys/gdot_rsa",
        "remote_dir": "/data/logs",
        "file_extensions": [".dat"],
        "decoder": "asc3",
        "mode": "passive"
    }'
)
WHERE signal_id = 'GDOT-0142';
```

### Database Schema

Collection configuration is stored in the `signal.metadata` JSONB column, not in a separate table. The `CollectorService` reads collection config from the `"collection"` key:

```sql
-- signal table (relevant columns)
-- signal_id  TEXT PRIMARY KEY
-- metadata   JSONB         -- collection config stored here
-- enabled    BOOLEAN       -- must be true for polling

-- Example metadata->collection structure:
-- {
--     "method": "ftp_pull",
--     "protocol": "sftp",
--     "host": "192.168.1.100",
--     "username": "atspm",
--     "password": "<encrypted>",
--     "remote_dir": "/data/logs",
--     "decoder": "asc3",
--     "ssh_key_path": "/etc/tsigma/keys/controller_rsa"
-- }
```

See also the `polling_checkpoint` table which tracks per-signal ingestion state.

---

## Example Deployments

### Small Agency (10 Signals)

**Estimated Load**: 10 signals × 1 hour interval = ~6 seconds of FTP activity per hour

**Scaling**: Single TSIGMA instance with default settings

---

### Medium Agency (500 Signals)

**Estimated Load**: 500 signals × 1 hour interval = ~5 minutes of FTP activity per hour

**Scaling**: Single TSIGMA instance (Python 3.14 free-threaded handles parallelism)

---

### Large Agency (5,000 Signals)

**Architecture**:
```
TSIGMA Instance 1 (Region 1-10)
    └─ Polls 2,500 signals

TSIGMA Instance 2 (Region 11-20)
    └─ Polls 2,500 signals

Shared PostgreSQL Database
```

**Estimated Load**: 5,000 signals × 1 hour interval = ~50 minutes of FTP activity per hour

---

**Document Version**: 1.1
**Last Updated**: 2026-04-22
**Owner**: OpenPhase Labs
