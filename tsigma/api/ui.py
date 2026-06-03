"""
Web UI routes for TSIGMA.

Serves Jinja2-rendered HTML pages. All data fetching happens
client-side via JavaScript calling the REST API.

Authentication is enforced server-side:
- /login is always public (unauthenticated users need to reach it).
- /admin/* routes require admin role.
- All other pages use the configurable access policy for "ui",
  which defaults to "authenticated".
"""

import logging
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..auth.dependencies import require_access, require_admin
from ..config import settings
from ..theming.resolver import resolve_template_dirs
from ..theming.tokens import token_style_for

logger = logging.getLogger(__name__)

# --- Sub-routers by access level -------------------------------------------

# Public routes (login page must be reachable without auth)
_public_router = APIRouter(tags=["ui"])

# Routes governed by the "ui" access policy (admin-configurable)
_authenticated_router = APIRouter(
    tags=["ui"],
    dependencies=[Depends(require_access("ui"))],
)

# Admin-only routes
_admin_router = APIRouter(
    tags=["ui"],
    dependencies=[Depends(require_admin)],
)

# Templates directory is tsigma/templates/ (sibling to api/). The active theme
# (TSIGMA_THEME) is resolved once at startup: an agency theme's templates shadow
# the defaults via the ChoiceLoader, falling back to these built-in defaults.
_template_dir = Path(__file__).resolve().parent.parent / "templates"
_themes_root = _template_dir.parent.parent / "themes"
_template_dirs = resolve_template_dirs(
    settings.theme, themes_root=_themes_root, default_templates=_template_dir
)
templates = Jinja2Templates(directory=[str(d) for d in _template_dirs])
# base.html uses Flask's get_flashed_messages(); FastAPI's Jinja2 doesn't
# provide it. Flash messaging isn't implemented, so register a no-op that
# accepts the with_categories kwarg and returns no messages.
templates.env.globals["get_flashed_messages"] = lambda with_categories=False: []
# Per-theme/per-mode CSS custom properties, rendered once and injected inline
# in base.html <head> (after tailwind.css) so theme + light/dark values win.
templates.env.globals["theme_token_style"] = token_style_for(
    settings.theme, themes_root=_themes_root
)


# --- Public ----------------------------------------------------------------

@_public_router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Login page."""
    return templates.TemplateResponse(request, "login.html")


# --- Authenticated (access policy: "ui") -----------------------------------

@_authenticated_router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page."""
    return templates.TemplateResponse(request, "pages/dashboard.html")


@_authenticated_router.get("/signals", response_class=HTMLResponse)
async def signals_list(request: Request):
    """Signal list page."""
    return templates.TemplateResponse(request, "pages/signals/index.html")


@_authenticated_router.get("/signals/{signal_id}", response_class=HTMLResponse)
async def signal_detail(request: Request, signal_id: str):
    """Signal detail page."""
    return templates.TemplateResponse(
        request,
        "pages/signals/detail.html",
        {"signal_id": signal_id},
    )


@_authenticated_router.get("/reports", response_class=HTMLResponse)
async def reports_list(request: Request):
    """Report selection page."""
    return templates.TemplateResponse(request, "pages/reports/index.html")


@_authenticated_router.get("/reports/{report_name}", response_class=HTMLResponse)
async def report_viewer(request: Request, report_name: str):
    """Report viewer page."""
    signal_id = request.query_params.get("signal_id", "")
    return templates.TemplateResponse(
        request,
        "pages/reports/viewer.html",
        {"report_name": report_name, "signal_id": signal_id},
    )


# --- Admin only ------------------------------------------------------------

@_admin_router.get("/admin/users", response_class=HTMLResponse)
async def admin_users(request: Request):
    """User management page (admin only)."""
    return templates.TemplateResponse(request, "pages/admin/users.html")


@_admin_router.get("/admin/settings", response_class=HTMLResponse)
async def admin_settings(request: Request):
    """System settings page (admin only)."""
    return templates.TemplateResponse(request, "pages/admin/settings.html")


# --- Aggregate router (imported by app.py as `ui_router`) ------------------
router = APIRouter()
router.include_router(_public_router)
router.include_router(_authenticated_router)
router.include_router(_admin_router)
