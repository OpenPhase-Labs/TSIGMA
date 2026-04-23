# Testing Strategy

> Part of [TSIGMA Architecture](../ARCHITECTURE.md)

---

## Test Organization

```
tests/
├── conftest.py                            # Shared fixtures
├── unit/                                  # No database, no network
│   ├── test_access_policy_dependency.py
│   ├── test_analytics_api.py
│   ├── test_analytics_schemas.py
│   ├── test_api_corridors.py
│   ├── test_api_keys.py
│   ├── test_api_reference.py
│   ├── test_api_regions.py
│   ├── test_api_reports.py
│   ├── test_api_routes.py
│   ├── test_app.py
│   ├── test_approach_schemas.py
│   ├── test_approaches_api.py
│   ├── test_auth_dependencies.py
│   ├── test_auth_models.py
│   ├── test_auth_oauth2.py
│   ├── test_auth_oidc.py
│   ├── test_auth_passwords.py
│   ├── test_auth_provisioning.py
│   ├── test_auth_router.py
│   ├── test_auth_schemas.py
│   ├── test_auth_seed.py
│   ├── test_auth_sessions.py
│   ├── test_collection_api.py
│   ├── test_collection_sdk.py
│   ├── test_collector_service.py
│   ├── test_config.py
│   ├── test_config_resolver.py
│   ├── test_credential_redaction.py
│   ├── test_crypto.py
│   ├── test_csrf.py
│   ├── test_cycle_aggregates.py
│   ├── test_database_facade.py
│   ├── test_database_init.py
│   ├── test_decoder_asc3.py
│   ├── test_decoder_auto.py
│   ├── test_decoder_csv.py
│   ├── test_decoder_maxtime.py
│   ├── test_decoder_openphase.py
│   ├── test_decoder_peek.py
│   ├── test_decoder_sdk.py
│   ├── test_decoder_siemens.py
│   ├── test_dependencies.py
│   ├── test_detector_schemas.py
│   ├── test_detectors_api.py
│   ├── test_dialect_helper.py
│   ├── test_directory_watch.py
│   ├── test_event_model_validation.py
│   ├── test_ftp_pull.py
│   ├── test_graphql.py
│   ├── test_http_pull.py
│   ├── test_jurisdiction_schemas.py
│   ├── test_jurisdictions_api.py
│   ├── test_logging.py
│   ├── test_main.py
│   ├── test_middleware.py
│   ├── test_models.py
│   ├── test_mqtt_listener.py
│   ├── test_nats_listener.py
│   ├── test_notifications.py
│   ├── test_rate_limit_middleware.py
│   ├── test_rate_limiter.py
│   ├── test_registries.py
│   ├── test_report_execute.py
│   ├── test_report_sdk.py
│   ├── test_report_sdk_cycles.py
│   ├── test_report_with_data.py
│   ├── test_reports.py
│   ├── test_reports_aggregate.py
│   ├── test_scheduler_jobs.py
│   ├── test_scheduler_service.py
│   ├── test_settings_api.py
│   ├── test_settings_service.py
│   ├── test_signal_schemas.py
│   ├── test_signals_api.py
│   ├── test_storage.py
│   ├── test_system_setting_model.py
│   ├── test_tcp_server.py
│   ├── test_udp_server.py
│   ├── test_ui.py
│   ├── test_validation_config.py
│   ├── test_validation_registry.py
│   ├── test_validation_sdk.py
│   ├── test_validation_service.py
│   ├── test_validator_schema_range.py
│   └── test_valkey_sessions.py
└── integration/                           # Requires database (skip unless --integration)
    ├── test_api_signals.py
    ├── test_database_facade_integration.py
    ├── test_database_init_integration.py
    └── test_signals_db_integration.py
```

## Test Configuration

```toml
# pyproject.toml

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24",
    "pytest-cov>=6.0",
    "httpx",
    "ruff>=0.9",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = "-v --strict-markers --tb=short --cov=tsigma --cov-report=term-missing --cov-report=html"
markers = [
    "integration: requires a running PostgreSQL database (set TSIGMA_TEST_DB_URL)",
]
```

## Running Tests

```bash
# All unit tests (no database required)
pytest tests/unit/ -v

# With coverage
pytest tests/ --cov=tsigma --cov-report=term-missing

# Integration tests (requires PostgreSQL)
pytest tests/integration/ -v

# A specific test file
pytest tests/unit/test_validator_schema_range.py -v
```

## TDD Workflow

TSIGMA follows strict TDD:

1. **RED** — Write failing tests first (import errors or assertion failures)
2. **GREEN** — Implement the minimum code to pass
3. **REFACTOR** — Clean up while keeping tests green

## Example Tests

**Unit Test (Decoder):**
```python
def test_decode_phase_event():
    raw = bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])
    decoder = ASC3Decoder()

    events = decoder.decode(raw)

    assert len(events) == 1
    assert events[0].event_code == 1
```

**Integration Test (API):**
```python
@pytest.mark.asyncio
async def test_create_signal(client: AsyncClient, auth_headers: dict):
    response = await client.post(
        "/api/v1/signals",
        json={"signal_id": "SIG-001", "name": "Test Signal"},
        headers=auth_headers,
    )

    assert response.status_code == 201
    data = response.json()
    assert data["signal_id"] == "SIG-001"
```

**Fixtures:**
```python
from fastapi.testclient import TestClient

@pytest.fixture
def client():
    return TestClient(app)

# Example — add to conftest.py if your tests need authenticated requests
@pytest.fixture
async def auth_headers(client):
    response = await client.post("/api/v1/auth/login", json={
        "username": "admin",
        "password": "testpassword"
    })
    return {"Cookie": response.headers.get("set-cookie")}
```
