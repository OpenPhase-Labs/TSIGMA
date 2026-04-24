"""
TSIGMA FastAPI Application.

Main application entry point with component lifecycle management.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware

import tsigma.database.db as _db_module

from .api.graphql.schema import graphql_router
from .api.ui import router as ui_router
from .api.v1.analytics import router as analytics_router
from .api.v1.approaches import router as approaches_router
from .api.v1.collection import router as collection_router
from .api.v1.corridors import router as corridors_router
from .api.v1.detectors import router as detectors_router
from .api.v1.jurisdictions import router as jurisdictions_router
from .api.v1.reference import router as reference_router
from .api.v1.regions import router as regions_router
from .api.v1.reports import router as reports_router
from .api.v1.routes import router as routes_router
from .api.v1.settings import router as settings_router
from .api.v1.signals import router as signals_router
from .auth.dependencies import require_access
from .auth.registry import AuthProviderRegistry
from .auth.router import router as auth_router
from .auth.seed import seed_admin
from .auth.sessions import InMemorySessionStore, ValkeySessionStore
from .config import settings
from .database.db import DatabaseFacade
from .logging import setup_logging
from .middleware import (
    LoggingMiddleware,
    RateLimitMiddleware,
    RequestIDMiddleware,
    SecurityHeadersMiddleware,
    TimingMiddleware,
)
from .rate_limiter import create_rate_limiter
from .settings_service import seed_system_settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.

    Handles startup and shutdown for all TSIGMA components.
    Initializes the DatabaseFacade and sets the global db_facade
    so that get_session() can provide sessions to route handlers.
    """
    # Startup
    setup_logging(settings.log_level, settings.log_format)

    if (settings.enable_api or settings.enable_collector
            or settings.enable_scheduler):
        facade = DatabaseFacade(
            settings.db_type,
            user=settings.pg_user,
            password=settings.pg_password,
            host=settings.pg_host,
            port=settings.pg_port,
            database=settings.pg_database,
        )
        await facade.connect()
        _db_module.db_facade = facade

        # Initialize session store — Valkey if configured, in-memory fallback
        if settings.valkey_url:
            import valkey.asyncio as valkey
            valkey_client = valkey.from_url(settings.valkey_url, decode_responses=False)
            app.state.valkey_client = valkey_client
            app.state.session_store = ValkeySessionStore(
                client=valkey_client,
                ttl_minutes=settings.auth_session_ttl_minutes,
            )
            logger.info("Session store: Valkey (%s)", settings.valkey_url)
        else:
            app.state.valkey_client = None
            store = InMemorySessionStore(
                ttl_minutes=settings.auth_session_ttl_minutes,
            )
            app.state.session_store = store
            logger.warning("Session store: in-memory (no valkey_url configured)")

            # Periodic cleanup for in-memory store — expired sessions and
            # CSRF tokens don't auto-expire like they do in Valkey.
            async def _cleanup_loop() -> None:
                import asyncio
                while True:
                    await asyncio.sleep(60)
                    await store.cleanup()

            app.state._cleanup_task = asyncio.create_task(_cleanup_loop())

        # Seed default admin user (local auth mode only)
        # and default system settings (access policies, etc.)
        async with facade._session_factory() as session:
            try:
                await seed_admin(session)
                await seed_system_settings(session)
                await session.commit()
            except Exception:
                await session.rollback()
                raise

        # Upgrade rate limiter to Valkey backend if available
        if settings.valkey_url and hasattr(app.state, "rate_limiter"):
            from .rate_limiter import ValkeyRateLimiterBackend
            app.state.rate_limiter.set_backend(
                ValkeyRateLimiterBackend(valkey_client)
            )
            logger.info("Rate limiter: upgraded to Valkey backend")

        # Initialize active auth provider and mount its routes
        provider_cls = AuthProviderRegistry.get(settings.auth_mode)
        provider = provider_cls()
        await provider.initialize()
        app.state.auth_provider = provider
        app.include_router(
            provider.get_router(), prefix="/api/v1/auth", tags=["auth"]
        )

    # Initialize notification providers
    from tsigma.notifications.registry import initialize_providers
    await initialize_providers(settings)

    if settings.enable_collector:
        from .collection.service import CollectorService
        from .collection.sources import SignalDeviceSource
        from .collection.targets import ControllerTarget

        controller_source = SignalDeviceSource(
            poll_interval_seconds=settings.collector_poll_interval,
            target=ControllerTarget(),
        )
        collector = CollectorService(
            facade._session_factory,
            settings,
            sources=[controller_source],
        )
        await collector.start()
        app.state.collector = collector

    if settings.enable_scheduler:
        from .scheduler.service import SchedulerService

        scheduler = SchedulerService(session_factory=facade._session_factory)
        await scheduler.start()
        scheduler.load_registry()
        app.state.scheduler = scheduler

    if settings.validation_enabled:
        from .validation.service import ValidationService

        validation_svc = ValidationService(facade._session_factory, settings)
        await validation_svc.start()
        app.state.validation_service = validation_svc

    yield

    # Shutdown — validation stops first
    if settings.validation_enabled and hasattr(app.state, "validation_service"):
        await app.state.validation_service.stop()

    # Shutdown — scheduler stops next (kills running jobs),
    # then collector unregisters from JobRegistry
    if settings.enable_scheduler and hasattr(app.state, "scheduler"):
        await app.state.scheduler.stop()

    if settings.enable_collector and hasattr(app.state, "collector"):
        await app.state.collector.stop()

    # Cancel in-memory session cleanup task
    cleanup_task = getattr(app.state, "_cleanup_task", None)
    if cleanup_task is not None:
        cleanup_task.cancel()

    # Close Valkey connection
    if getattr(app.state, "valkey_client", None):
        await app.state.valkey_client.aclose()

    if _db_module.db_facade:
        await _db_module.db_facade.disconnect()
        _db_module.db_facade = None


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.

    Returns:
        Configured FastAPI application instance.
    """
    app = FastAPI(
        title="TSIGMA",
        description="Traffic Signal Intelligence: Gathering Metrics & Analytics",
        version="1.0.0",
        lifespan=lifespan,
    )

    # --- CORS ----------------------------------------------------------------
    if settings.cors_origins:
        origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
        )

    # --- Rate limiter (in-memory default; upgraded to Valkey in lifespan) -----
    rate_limiter = create_rate_limiter(
        login_limit=settings.rate_limit_login,
        read_limit=settings.rate_limit_read,
        write_limit=settings.rate_limit_write,
    )
    app.state.rate_limiter = rate_limiter

    # --- Middleware (order matters - first added = outermost) -----------------
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(RateLimitMiddleware, limiter=rate_limiter)
    app.add_middleware(LoggingMiddleware)
    app.add_middleware(TimingMiddleware)
    app.add_middleware(RequestIDMiddleware)
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    # --- Global exception handlers -------------------------------------------
    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError):
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": str(exc)},
        )

    @app.exception_handler(RuntimeError)
    async def runtime_error_handler(request: Request, exc: RuntimeError):
        logger.error("Runtime error: %s", exc, exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"},
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.error("Unhandled exception: %s", exc, exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"},
        )

    # --- Health endpoints (no auth, always available) ------------------------
    @app.get("/health", tags=["system"])
    async def health():
        """Liveness probe — returns 200 if the process is running."""
        return {"status": "ok"}

    @app.get("/ready", tags=["system"])
    async def readiness():
        """Readiness probe — returns 200 if database is connected."""
        if _db_module.db_facade is None:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "not ready", "reason": "database not connected"},
            )
        return {"status": "ready"}

    # --- API routers ---------------------------------------------------------
    if settings.enable_api:
        app.include_router(signals_router, prefix="/api/v1/signals", tags=["signals"])
        app.include_router(approaches_router, prefix="/api/v1", tags=["approaches"])
        app.include_router(detectors_router, prefix="/api/v1", tags=["detectors"])
        app.include_router(
            jurisdictions_router,
            prefix="/api/v1/jurisdictions",
            tags=["jurisdictions"],
        )
        app.include_router(analytics_router, prefix="/api/v1/analytics", tags=["analytics"])
        app.include_router(auth_router, prefix="/api/v1/auth", tags=["auth"])
        app.include_router(collection_router, prefix="/api/v1", tags=["collection"])
        app.include_router(reference_router, prefix="/api/v1", tags=["reference"])
        app.include_router(regions_router, prefix="/api/v1/regions", tags=["regions"])
        app.include_router(corridors_router, prefix="/api/v1/corridors", tags=["corridors"])
        app.include_router(routes_router, prefix="/api/v1", tags=["routes"])
        app.include_router(reports_router, prefix="/api/v1", tags=["reports"])
        app.include_router(settings_router, prefix="/api/v1/settings", tags=["settings"])
        app.include_router(
            graphql_router,
            prefix="/graphql",
            tags=["graphql"],
            dependencies=[Depends(require_access("analytics"))],
        )

    # --- Static files --------------------------------------------------------
    static_dir = Path(__file__).resolve().parent / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # --- Web UI routes (must be AFTER API routes to avoid path conflicts) ----
    if settings.enable_api:
        app.include_router(ui_router)

    return app


# Application instance
app = create_app()
