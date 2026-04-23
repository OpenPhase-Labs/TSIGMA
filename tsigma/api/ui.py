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

# Templates directory is tsigma/templates/ (sibling to api/)
_template_dir = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(_template_dir))


# --- Public ----------------------------------------------------------------

@_public_router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Login page."""
    return templates.TemplateResponse("login.html", {"request": request})


# --- Authenticated (access policy: "ui") -----------------------------------

@_authenticated_router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page."""
    return templates.TemplateResponse("pages/dashboard.html", {"request": request})


@_authenticated_router.get("/signals", response_class=HTMLResponse)
async def signals_list(request: Request):
    """Signal list page."""
    return templates.TemplateResponse("pages/signals/index.html", {"request": request})


@_authenticated_router.get("/signals/{signal_id}", response_class=HTMLResponse)
async def signal_detail(request: Request, signal_id: str):
    """Signal detail page."""
    return templates.TemplateResponse("pages/signals/detail.html", {
        "request": request,
        "signal_id": signal_id,
    })


@_authenticated_router.get("/reports", response_class=HTMLResponse)
async def reports_list(request: Request):
    """Report selection page."""
    return templates.TemplateResponse("pages/reports/index.html", {"request": request})


@_authenticated_router.get("/reports/{report_name}", response_class=HTMLResponse)
async def report_viewer(request: Request, report_name: str):
    """Report viewer page."""
    signal_id = request.query_params.get("signal_id", "")
    return templates.TemplateResponse("pages/reports/viewer.html", {
        "request": request,
        "report_name": report_name,
        "signal_id": signal_id,
    })


# --- Admin only ------------------------------------------------------------

@_admin_router.get("/admin/users", response_class=HTMLResponse)
async def admin_users(request: Request):
    """User management page (admin only)."""
    return templates.TemplateResponse("pages/admin/users.html", {"request": request})


@_admin_router.get("/admin/settings", response_class=HTMLResponse)
async def admin_settings(request: Request):
    """System settings page (admin only)."""
    return templates.TemplateResponse("pages/admin/settings.html", {"request": request})


# --- Aggregate router (imported by app.py as `ui_router`) ------------------
router = APIRouter()
router.include_router(_public_router)
router.include_router(_authenticated_router)
router.include_router(_admin_router)
