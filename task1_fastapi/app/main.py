from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, status

from .dependencies import get_pg_connection
from .settings import PostgresSettings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = PostgresSettings()
    try:
        pool = await asyncpg.create_pool(
            host=settings.host,
            port=settings.port,
            user=settings.user,
            password=settings.password.get_secret_value(),
            database=settings.database,
            min_size=settings.min_size,
            max_size=settings.max_size,
            command_timeout=settings.command_timeout,
        )
    except asyncpg.PostgresError as exc:
        raise RuntimeError("Failed to initialize PostgreSQL connection pool") from exc

    app.state.pg_pool = pool
    try:
        yield
    finally:
        await pool.close()


app = FastAPI(lifespan=lifespan)


@app.get("/db/version")
async def get_db_version(connection: asyncpg.Connection = Depends(get_pg_connection)) -> dict[str, Any]:
    try:
        record = await connection.fetchrow("SELECT version() AS version")
    except asyncpg.PostgresError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch database version",
        ) from exc
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database version not found",
        )
    return {"version": record["version"]}

