from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import Depends, HTTPException, Request, status

import asyncpg


async def get_pg_pool(request: Request) -> asyncpg.pool.Pool:
    pool = getattr(request.app.state, "pg_pool", None)
    if pool is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection pool is not initialized",
        )
    return pool


async def get_pg_connection(
    pool: asyncpg.pool.Pool = Depends(get_pg_pool),
) -> AsyncIterator[asyncpg.connection.Connection]:
    try:
        async with pool.acquire() as connection:
            yield connection
    except asyncpg.PostgresError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to acquire database connection",
        ) from exc

