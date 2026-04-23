"""
Generic CRUD router factory.

Generates FastAPI routers with list/get/create/update/delete endpoints
for any SQLAlchemy model + Pydantic schema combination.

Usage:
    # Full CRUD (all 5 endpoints):
    router = crud_router(
        model=DirectionType,
        create_schema=DirectionTypeCreate,
        update_schema=DirectionTypeUpdate,
        response_schema=DirectionTypeResponse,
        pk_field="direction_type_id",
        prefix="/direction-types",
        resource_name="DirectionType",
    )

    # Partial — only get/update/delete (custom list/create elsewhere):
    router = crud_router(
        model=RouteSignal,
        ...,
        operations={"get", "update", "delete"},
    )
"""

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ...auth.dependencies import require_access, require_admin
from ...auth.sessions import SessionData
from ...dependencies import get_audited_session, get_session

_ALL_OPS = frozenset({"list", "get", "create", "update", "delete"})


def _add_list_endpoint(router, model, prefix, response_schema, read_scope):
    """Register a GET list endpoint on the router."""

    @router.get(f"{prefix}/", response_model=list[response_schema])
    async def list_all(
        session: AsyncSession = Depends(get_session),
        _access=Depends(require_access(read_scope)),
    ):
        result = await session.execute(select(model))
        return result.scalars().all()


def _add_get_endpoint(router, prefix, response_schema, read_scope, get_or_404):
    """Register a GET-by-PK endpoint on the router."""

    @router.get(f"{prefix}/{{pk}}", response_model=response_schema)
    async def get_one(
        pk: str,
        session: AsyncSession = Depends(get_session),
        _access=Depends(require_access(read_scope)),
    ):
        return await get_or_404(pk, session)


def _add_create_endpoint(
    router, model, prefix, response_schema, create_schema,
    pk_field, pk_column, resource_name, user_supplied_pk,
):
    """Register a POST create endpoint on the router."""

    @router.post(
        f"{prefix}/",
        response_model=response_schema,
        status_code=status.HTTP_201_CREATED,
    )
    async def create(
        body: create_schema,
        session: AsyncSession = Depends(get_audited_session),
        _: SessionData = Depends(require_admin),
    ):
        data = body.model_dump()

        if user_supplied_pk:
            pk_value = data.get(pk_field)
            existing = await session.execute(
                select(model).where(pk_column == pk_value)
            )
            if existing.scalar_one_or_none() is not None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"{resource_name} {pk_value} already exists",
                )

        obj = model(**data)
        session.add(obj)
        await session.flush()
        return obj


def _add_update_endpoint(router, prefix, response_schema, update_schema, get_or_404):
    """Register a PUT update endpoint on the router."""

    @router.put(f"{prefix}/{{pk}}", response_model=response_schema)
    async def update(
        pk: str,
        body: update_schema,
        session: AsyncSession = Depends(get_audited_session),
        _: SessionData = Depends(require_admin),
    ):
        obj = await get_or_404(pk, session)
        update_data = body.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(obj, field, value)
        await session.flush()
        return obj


def _add_delete_endpoint(router, prefix, get_or_404):
    """Register a DELETE endpoint on the router."""

    @router.delete(f"{prefix}/{{pk}}", status_code=status.HTTP_204_NO_CONTENT)
    async def delete(
        pk: str,
        session: AsyncSession = Depends(get_audited_session),
        _: SessionData = Depends(require_admin),
    ):
        obj = await get_or_404(pk, session)
        await session.delete(obj)
        await session.flush()


def crud_router(
    *,
    model: type,
    create_schema: type | None = None,
    update_schema: type | None = None,
    response_schema: type,
    pk_field: str,
    prefix: str,
    resource_name: str,
    read_scope: str = "signal_detail",
    user_supplied_pk: bool = False,
    operations: set[str] | None = None,
) -> APIRouter:
    """
    Build an APIRouter with standard CRUD endpoints.

    Args:
        model: SQLAlchemy ORM model class.
        create_schema: Pydantic schema for create requests.
        update_schema: Pydantic schema for update requests.
        response_schema: Pydantic schema for responses.
        pk_field: Name of the primary key column on the model.
        prefix: URL prefix (e.g., "/direction-types").
        resource_name: Human-readable name for error messages.
        read_scope: Access policy scope for GET endpoints.
        user_supplied_pk: If True, create checks for conflicts.
        operations: Subset of {"list", "get", "create", "update", "delete"}.

    Returns:
        APIRouter with the requested endpoints.
    """
    ops = operations if operations is not None else _ALL_OPS
    router = APIRouter()
    pk_column = getattr(model, pk_field)

    async def _get_or_404(pk_value: Any, session: AsyncSession):
        result = await session.execute(
            select(model).where(pk_column == pk_value)
        )
        obj = result.scalar_one_or_none()
        if obj is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"{resource_name} {pk_value} not found",
            )
        return obj

    if "list" in ops:
        _add_list_endpoint(router, model, prefix, response_schema, read_scope)
    if "get" in ops:
        _add_get_endpoint(router, prefix, response_schema, read_scope, _get_or_404)
    if "create" in ops:
        _add_create_endpoint(
            router, model, prefix, response_schema, create_schema,
            pk_field, pk_column, resource_name, user_supplied_pk,
        )
    if "update" in ops:
        _add_update_endpoint(router, prefix, response_schema, update_schema, _get_or_404)
    if "delete" in ops:
        _add_delete_endpoint(router, prefix, _get_or_404)

    return router
