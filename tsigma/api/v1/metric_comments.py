"""
Metric comments API endpoints.

User-authored annotations on a signal's charts, optionally anchored to a point
or a range on the time axis. Reads respect the 'comments' access policy;
create/update/delete require an authenticated caller and are restricted to the
comment's author or an admin.

Every route here is hand-rolled: ``crud_router`` hard-wires
``Depends(require_admin)`` on its write endpoints, so it cannot express
author-or-admin scoping.

Authorship is a denormalised snapshot on the row (``author_uuid`` +
``author_username``), captured from the session at creation. There is no
foreign key to ``auth_user`` and no join: a comment outlives its author.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import get_current_user, require_access
from ...auth.sessions import SessionData
from ...dependencies import get_session
from ...models import MetricComment, MetricCommentMetricType, MetricType, Signal
from .helpers import get_or_404
from .schemas import UPDATE_REQUIRED_MSG

# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


ANCHOR_END_NEEDS_START = "anchor_end requires anchor_start"
ANCHOR_ORDER_MSG = "anchor_end must not precede anchor_start"


def _validate_anchor_pair(
    anchor_start: Optional[datetime], anchor_end: Optional[datetime]
) -> None:
    """Enforce Decision 1's three valid anchor states. Raises ValueError."""
    if anchor_end is not None and anchor_start is None:
        raise ValueError(ANCHOR_END_NEEDS_START)
    if anchor_start is not None and anchor_end is not None and anchor_end < anchor_start:
        raise ValueError(ANCHOR_ORDER_MSG)


class MetricCommentCreate(BaseModel):
    """Schema for creating a metric comment. The author comes from the session."""

    signal_id: str = Field(..., min_length=1)
    text: str = Field(..., min_length=1)
    anchor_start: Optional[datetime] = None
    anchor_end: Optional[datetime] = None

    @model_validator(mode="after")
    def check_anchor_states(self) -> "MetricCommentCreate":
        """Reject the fourth, undefined anchor state and inverted ranges."""
        _validate_anchor_pair(self.anchor_start, self.anchor_end)
        return self


class MetricCommentUpdate(BaseModel):
    """Schema for updating an existing metric comment (partial update)."""

    text: Optional[str] = Field(None, min_length=1)
    anchor_start: Optional[datetime] = None
    anchor_end: Optional[datetime] = None

    @model_validator(mode="after")
    def check_at_least_one_field(self) -> "MetricCommentUpdate":
        """Ensure at least one field is provided for update."""
        values = self.model_dump(exclude_unset=True)
        if not values:
            raise ValueError(UPDATE_REQUIRED_MSG)
        return self


class MetricCommentResponse(BaseModel):
    """Metric comment returned in API responses, with the author's username."""

    model_config = {"from_attributes": True}

    id: UUID
    signal_id: str
    text: str
    author_uuid: UUID
    author_username: str
    anchor_start: Optional[datetime] = None
    anchor_end: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class MetricCommentMetricTypeCreate(BaseModel):
    """Schema for attaching a metric type to a comment."""

    metric_type_key: str = Field(..., min_length=1)


class MetricCommentMetricTypeResponse(BaseModel):
    """Comment/metric-type membership returned in API responses."""

    model_config = {"from_attributes": True}

    metric_type_key: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

RESOURCE_NAME = "MetricComment"


def _to_response(comment: MetricComment) -> MetricCommentResponse:
    """Build a response from a comment row; authorship is carried on the row."""
    return MetricCommentResponse.model_validate(comment)


async def _get_comment(session: AsyncSession, comment_id: UUID) -> MetricComment:
    """Fetch a comment by id, or raise 404."""
    return await get_or_404(
        session, MetricComment, MetricComment.id, comment_id, RESOURCE_NAME
    )


def _require_author_or_admin(comment: MetricComment, user: SessionData) -> None:
    """Allow the comment's own author or an admin; 403 for anyone else."""
    if not (comment.author_uuid == user.user_id or user.role == "admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Comment author or admin role required",
        )


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter()


@router.get("/", response_model=list[MetricCommentResponse])
async def list_metric_comments(
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("comments")),
):
    """List all metric comments, oldest first."""
    result = await session.execute(
        select(MetricComment).order_by(MetricComment.created_at)
    )
    return [_to_response(comment) for comment in result.scalars().all()]


@router.get("/{comment_id}", response_model=MetricCommentResponse)
async def get_metric_comment(
    comment_id: UUID,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("comments")),
):
    """Get a single metric comment."""
    comment = await _get_comment(session, comment_id)
    return _to_response(comment)


@router.post(
    "/",
    response_model=MetricCommentResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_metric_comment(
    body: MetricCommentCreate,
    session: AsyncSession = Depends(get_session),
    user: SessionData = Depends(get_current_user),
):
    """Create a metric comment authored by the calling user."""
    await get_or_404(session, Signal, Signal.signal_id, body.signal_id, "Signal")

    comment = MetricComment(
        signal_id=body.signal_id,
        text=body.text,
        author_uuid=user.user_id,
        author_username=user.username,
        anchor_start=body.anchor_start,
        anchor_end=body.anchor_end,
    )
    session.add(comment)
    await session.flush()

    return _to_response(comment)


@router.put("/{comment_id}", response_model=MetricCommentResponse)
async def update_metric_comment(
    comment_id: UUID,
    body: MetricCommentUpdate,
    session: AsyncSession = Depends(get_session),
    user: SessionData = Depends(get_current_user),
):
    """Update a metric comment (author or admin only)."""
    comment = await _get_comment(session, comment_id)
    _require_author_or_admin(comment, user)

    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(comment, field, value)

    # A partial update can reach the invalid state without either anchor being
    # in the payload -- clearing anchor_start while a stored anchor_end remains.
    # The merged row is the only place that is visible.
    try:
        _validate_anchor_pair(comment.anchor_start, comment.anchor_end)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc

    await session.flush()

    return _to_response(comment)


@router.delete("/{comment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_metric_comment(
    comment_id: UUID,
    session: AsyncSession = Depends(get_session),
    user: SessionData = Depends(get_current_user),
):
    """Delete a metric comment (author or admin only)."""
    comment = await get_or_404(
        session, MetricComment, MetricComment.id, comment_id, RESOURCE_NAME
    )
    _require_author_or_admin(comment, user)

    await session.delete(comment)


@router.get(
    "/{comment_id}/metric-types",
    response_model=list[MetricCommentMetricTypeResponse],
)
async def list_comment_metric_types(
    comment_id: UUID,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("comments")),
):
    """List the metric types a comment annotates."""
    await get_or_404(
        session, MetricComment, MetricComment.id, comment_id, RESOURCE_NAME
    )

    result = await session.execute(
        select(MetricCommentMetricType).where(
            MetricCommentMetricType.comment_id == comment_id
        )
    )
    return result.scalars().all()


@router.post(
    "/{comment_id}/metric-types",
    response_model=MetricCommentMetricTypeResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_comment_metric_type(
    comment_id: UUID,
    body: MetricCommentMetricTypeCreate,
    session: AsyncSession = Depends(get_session),
    user: SessionData = Depends(get_current_user),
):
    """Attach a metric type to a comment (author or admin only)."""
    comment = await get_or_404(
        session, MetricComment, MetricComment.id, comment_id, RESOURCE_NAME
    )
    _require_author_or_admin(comment, user)
    await get_or_404(
        session, MetricType, MetricType.key, body.metric_type_key, "MetricType"
    )

    assoc = MetricCommentMetricType(
        comment_id=comment_id,
        metric_type_key=body.metric_type_key,
    )
    session.add(assoc)
    await session.flush()

    return assoc


@router.delete(
    "/{comment_id}/metric-types/{metric_type_key}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_comment_metric_type(
    comment_id: UUID,
    metric_type_key: str,
    session: AsyncSession = Depends(get_session),
    user: SessionData = Depends(get_current_user),
):
    """Detach a metric type from a comment (author or admin only)."""
    comment = await get_or_404(
        session, MetricComment, MetricComment.id, comment_id, RESOURCE_NAME
    )
    _require_author_or_admin(comment, user)

    result = await session.execute(
        select(MetricCommentMetricType).where(
            MetricCommentMetricType.comment_id == comment_id,
            MetricCommentMetricType.metric_type_key == metric_type_key,
        )
    )
    assoc = result.scalar_one_or_none()

    if assoc is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Metric type {metric_type_key} not attached to comment {comment_id}",
        )

    await session.delete(assoc)
