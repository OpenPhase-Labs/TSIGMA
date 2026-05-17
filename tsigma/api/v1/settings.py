"""
System settings API endpoints.

Admin-only endpoints for reading and updating runtime-configurable
system settings stored in the database.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_admin
from ...auth.sessions import SessionData
from ...dependencies import get_session
from ...models.system_setting import SystemSetting
from ...models.system_setting_audit import SystemSettingAudit
from ...settings_service import (
    ACCESS_VALUES,
    LOCKED_CATEGORIES,
    settings_cache,
)
from ...settings_service import set as settings_set

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class SettingResponse(BaseModel):
    key: str
    value: str
    category: str
    description: str
    editable: bool
    updated_at: str | None = None
    updated_by: str | None = None


class SettingUpdate(BaseModel):
    value: str
    reason: str | None = None


class AccessPolicyResponse(BaseModel):
    analytics: str
    reports: str
    signal_detail: str
    health: str
    management: str


class AccessPolicyUpdate(BaseModel):
    analytics: str | None = None
    reports: str | None = None
    signal_detail: str | None = None
    health: str | None = None
    reason: str | None = None


# ---------------------------------------------------------------------------
# Response formatting helpers
# ---------------------------------------------------------------------------


def _setting_to_response(r) -> SettingResponse:
    """Convert a SystemSetting row to a SettingResponse."""
    return SettingResponse(
        key=r.key,
        value=r.value,
        category=r.category,
        description=r.description,
        editable=r.editable,
        updated_at=r.updated_at.isoformat() if r.updated_at else None,
        updated_by=r.updated_by,
    )


def _policies_to_response(policies: dict) -> AccessPolicyResponse:
    """Convert a policies dict to an AccessPolicyResponse."""
    return AccessPolicyResponse(
        analytics=policies.get("access_policy.analytics", "authenticated"),
        reports=policies.get("access_policy.reports", "authenticated"),
        signal_detail=policies.get(
            "access_policy.signal_detail", "authenticated",
        ),
        health=policies.get("access_policy.health", "authenticated"),
        management=policies.get(
            "access_policy.management", "authenticated",
        ),
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/", response_model=list[SettingResponse])
async def list_settings(
    category: str | None = None,
    session: AsyncSession = Depends(get_session),
    _admin: SessionData = Depends(require_admin),
):
    """List all system settings, optionally filtered by category."""
    query = select(SystemSetting)
    if category:
        query = query.where(SystemSetting.category == category)
    query = query.order_by(SystemSetting.key)

    result = await session.execute(query)
    rows = result.scalars().all()
    return [_setting_to_response(r) for r in rows]


@router.get("/access-policy", response_model=AccessPolicyResponse)
async def get_access_policy(
    session: AsyncSession = Depends(get_session),
    _admin: SessionData = Depends(require_admin),
):
    """Get current access policy settings."""
    policies = await settings_cache.get_by_category("access_policy", session)
    return _policies_to_response(policies)


@router.put("/access-policy", response_model=AccessPolicyResponse)
async def update_access_policy(
    body: AccessPolicyUpdate,
    session: AsyncSession = Depends(get_session),
    admin: SessionData = Depends(require_admin),
):
    """
    Update access policy settings.

    Only non-locked categories can be changed. The management category
    is always authenticated and cannot be modified.
    """
    dumped = body.model_dump()
    reason = dumped.pop("reason", None)
    updates = {k: v for k, v in dumped.items() if v is not None}

    if not updates:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="No fields to update",
        )

    # Validate all categories/values BEFORE any write so a failed PUT
    # leaves no audit rows behind.
    for category, value in updates.items():
        if category in LOCKED_CATEGORIES:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Category '{category}' cannot be modified",
            )
        if value not in ACCESS_VALUES:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=(
                    f"Invalid value '{value}' for {category}."
                    " Must be 'public' or 'authenticated'."
                ),
            )

    # Apply each update via the audit-aware set() helper. One audit row
    # per modified category.
    for category, value in updates.items():
        await settings_set(
            f"access_policy.{category}",
            value,
            session,
            changed_by=admin.username,
            reason=reason,
        )

    # Invalidate at the endpoint level too. `set()` invalidates the global
    # cache instance, but tests patch the module-local `settings_cache`
    # reference imported here, so the patched mock only observes the call
    # if we invalidate via this name as well.
    settings_cache.invalidate()

    # Return updated state
    policies = await settings_cache.get_by_category("access_policy", session)
    return _policies_to_response(policies)


@router.get("/{key}/audit")
async def get_setting_audit(
    key: str,
    skip: int = 0,
    limit: int = 50,
    session: AsyncSession = Depends(get_session),
    _admin: SessionData = Depends(require_admin),
):
    """
    Get audit trail for a system setting.

    Returns change history ordered by most recent first. Empty list
    (not 404) when no audit rows exist for the key.

    Args:
        key: System setting key (e.g. ``api.max_page_size``).
        skip: Number of records to skip (pagination).
        limit: Maximum number of records to return (default 50).

    Returns:
        List of audit dicts with keys: id, key, old_value, new_value,
        changed_at, changed_by, reason.
    """
    result = await session.execute(
        select(SystemSettingAudit)
        .where(SystemSettingAudit.key == key)
        .order_by(SystemSettingAudit.changed_at.desc())
        .offset(skip)
        .limit(limit)
    )
    rows = result.scalars().all()
    return [
        {
            "id": row.id,
            "key": row.key,
            "old_value": row.old_value,
            "new_value": row.new_value,
            "changed_at": row.changed_at.isoformat() if row.changed_at else None,
            "changed_by": row.changed_by,
            "reason": row.reason,
        }
        for row in rows
    ]


@router.put("/{setting_key:path}", response_model=SettingResponse)
async def update_setting(
    setting_key: str,
    body: SettingUpdate,
    session: AsyncSession = Depends(get_session),
    admin: SessionData = Depends(require_admin),
):
    """Update a single system setting by key."""
    result = await session.execute(
        select(SystemSetting).where(SystemSetting.key == setting_key)
    )
    setting = result.scalar_one_or_none()
    if not setting:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Setting '{setting_key}' not found",
        )
    if not setting.editable:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Setting '{setting_key}' is not editable",
        )

    # Validate access_policy values BEFORE writing so a 422 leaves no
    # audit row behind.
    if setting.category == "access_policy" and body.value not in ACCESS_VALUES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Invalid value '{body.value}'."
                " Must be 'public' or 'authenticated'."
            ),
        )

    # Route the write through the audit-aware set() helper. The non-registry
    # permissive branch mutates the in-memory `setting` row, so the response
    # below reflects the new value.
    await settings_set(
        setting_key,
        body.value,
        session,
        changed_by=admin.username,
        reason=body.reason,
    )

    # Invalidate at the endpoint level too. `set()` invalidates the global
    # cache instance, but tests patch the module-local `settings_cache`
    # reference imported here, so the patched mock only observes the call
    # if we invalidate via this name as well.
    settings_cache.invalidate()

    return _setting_to_response(setting)
