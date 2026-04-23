"""
Shared API helpers.

Common utilities used across multiple API endpoint modules.
"""

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


async def get_or_404(session: AsyncSession, model, pk_column, pk_value, resource_name: str):
    """
    Fetch a single record or raise 404 if not found.

    Args:
        session: SQLAlchemy async session.
        model: The SQLAlchemy model class (unused but kept for clarity).
        pk_column: The column to filter on (e.g., Signal.signal_id).
        pk_value: The value to match.
        resource_name: Human-readable name for the error message.

    Returns:
        The fetched model instance.

    Raises:
        HTTPException: 404 if not found.
    """
    result = await session.execute(select(model).where(pk_column == pk_value))
    obj = result.scalar_one_or_none()
    if obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"{resource_name} {pk_value} not found",
        )
    return obj
