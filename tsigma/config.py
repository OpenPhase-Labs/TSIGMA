"""
TSIGMA Configuration.

Pydantic settings with environment variable support.
Configuration priority: Environment Variables > YAML > Database
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    TSIGMA application settings.

    Loads from environment variables with TSIGMA_ prefix.
    """

    model_config = SettingsConfigDict(
        env_prefix="TSIGMA_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Database connection
    db_type: str = "postgresql"
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "tsigma"
    pg_user: str = "tsigma"
    pg_password: str = ""

    # Component toggles
    enable_api: bool = True
    enable_collector: bool = True
    enable_scheduler: bool = True

    # Event-log partition tuning (configurable at app init).
    # Used by TimescaleDB (chunk_time_interval) on PostgreSQL and by the
    # partition-management job for MS-SQL / Oracle / MySQL.  Integer days,
    # default 1 — daily partitions work well for multi-million-row daily
    # event volumes at signal scale.
    event_log_partition_interval_days: int = 1
    # How many future partitions to keep pre-created ahead of ``today`` on
    # MS-SQL / Oracle / MySQL.  Safety margin against scheduler outages.
    # No effect on PostgreSQL (TimescaleDB creates chunks on demand).
    partition_lookahead_days: int = 7
    # Drop partitions older than this many days on MS-SQL / Oracle / MySQL.
    # ``None`` disables auto-drop (partitions grow without bound — safest
    # default for pre-production).  Align with ``storage_retention`` when
    # ready to enable retention in production.
    partition_retention_days: int | None = None
    storage_warm_after: str = "7 days"
    storage_retention: str = "2 years"

    # Cold tier (On-Prem only)
    storage_cold_enabled: bool = False
    storage_cold_after: str = "6 months"
    storage_cold_path: str = "/var/lib/tsigma/cold"

    # File storage backend
    storage_backend: str = "filesystem"  # "filesystem" or "s3"
    storage_path: str = "/var/lib/tsigma/storage"
    storage_s3_bucket: str = ""
    storage_s3_region: str = "us-east-1"
    storage_s3_endpoint: str = ""
    storage_s3_access_key: str = ""
    storage_s3_secret_key: str = ""

    # Collector settings
    collector_max_concurrent: int = 50
    collector_poll_interval: int = 300

    # Checkpoint resilience
    checkpoint_future_tolerance_seconds: int = 300  # 5 minutes
    checkpoint_silent_cycles_threshold: int = 3  # alert after N silent cycles

    # Watchdog data-quality checks
    # Minimum raw-event volume (any event code) per signal over the last hour.
    # Below this threshold indicates likely comm failure, controller sleep,
    # or decoder issue.
    watchdog_low_event_count_threshold: int = 100
    # Maximum allowed gap (minutes) between the latest event for a signal and
    # now-UTC before the signal is flagged as having a data-window gap.
    watchdog_missing_window_minutes: int = 30
    # Minimum continuous-active duration (minutes) for a pedestrian detector
    # before it is flagged as stuck.
    watchdog_stuck_ped_minutes: int = 120
    # Number of standard deviations a phase's last-hour termination ratio
    # (gap-out / max-out / force-off) must exceed its 7-day baseline by in
    # order to be flagged as anomalous.
    watchdog_termination_anomaly_stddev: float = 3.0
    # Minimum detector ON events (code 82) per detector per hour during hours
    # where the approach has had at least one green phase. Below this
    # threshold flags a detector as having a low hit count.
    watchdog_low_hit_threshold: int = 5

    # API configuration
    api_host: str = "127.0.0.1"
    api_port: int = 8080
    debug: bool = False

    # CORS (comma-separated origins, empty = no CORS)
    cors_origins: str = ""

    # Logging
    log_level: str = "INFO"
    log_format: str = "json"

    # Aggregation pipeline
    aggregation_enabled: bool = True
    aggregation_interval_minutes: int = 15
    aggregation_lookback_hours: int = 2
    aggregation_coordination_tolerance_seconds: float = 2.0

    # Valkey (session store, cache, websockets)
    valkey_url: str = ""  # e.g. "redis://localhost:6379/0" — empty = in-memory fallback

    # Authentication
    auth_mode: str = "local"
    auth_admin_user: str = "admin"
    auth_admin_password: str = "changeme"
    auth_session_ttl_minutes: int = 480
    auth_cookie_name: str = "tsigma_session"
    auth_cookie_secure: bool = True
    hsts_preload: bool = False

    # OIDC settings (Azure AD / Entra ID)
    oidc_tenant_id: str = ""
    oidc_client_id: str = ""
    oidc_client_secret: str = ""
    oidc_scopes: str = "openid email profile"
    oidc_redirect_uri: str = ""
    oidc_admin_groups: str = ""

    # Notification providers (comma-separated list of active providers)
    notification_providers: str = ""  # e.g., "email,slack" or "email" or ""

    # Email notification settings
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_use_tls: bool = True
    notification_from_email: str = ""
    notification_to_emails: str = ""  # comma-separated

    # Slack notification settings
    slack_webhook_url: str = ""

    # MS Teams notification settings
    teams_webhook_url: str = ""

    # Validation pipeline
    validation_enabled: bool = True          # Master toggle
    validation_layer1_enabled: bool = True   # Schema/range (always recommended)
    validation_layer2_enabled: bool = False  # Temporal/anomaly (requires SLM)
    validation_layer3_enabled: bool = False  # Cross-signal (requires SLM + corridors)
    validation_batch_size: int = 5000        # Events per validation pass
    validation_interval: int = 60            # Seconds between validation runs

    # Generic OAuth2 settings
    oauth2_issuer_url: str = ""
    oauth2_client_id: str = ""
    oauth2_client_secret: str = ""
    oauth2_scopes: str = "openid email profile"
    oauth2_redirect_uri: str = ""
    oauth2_authorization_endpoint: str = ""
    oauth2_token_endpoint: str = ""
    oauth2_userinfo_endpoint: str = ""
    oauth2_admin_groups: str = ""
    oauth2_username_claim: str = "email"

    # Rate limiting (requests per minute)
    rate_limit_login: int = 5    # per minute per IP
    rate_limit_read: int = 100   # per minute per session
    rate_limit_write: int = 30   # per minute per session

    # Credential encryption
    secret_key: str = ""              # Fernet key (base64-encoded 32 bytes)
    secret_key_file: str = ""         # Path to file containing the key
    secret_key_vault_url: str = ""    # Vault URL for key retrieval (e.g., HashiCorp Vault)
    secret_key_vault_path: str = ""   # Vault secret path (e.g., "secret/data/tsigma")
    secret_key_vault_field: str = "secret_key"  # Field name within the vault secret


# Global settings instance
settings = Settings()
