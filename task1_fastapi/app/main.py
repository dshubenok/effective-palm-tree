from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

import asyncpg
import uvicorn
from fastapi import APIRouter, Depends, FastAPI, HTTPException, status

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


async def get_db_version(
    connection: Annotated[asyncpg.Connection, Depends(get_pg_connection)],
) -> dict[str, Any]:
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


def register_routes(app: FastAPI) -> None:
    router = APIRouter(prefix="/api")
    router.add_api_route(
        path="/db_version",
        endpoint=get_db_version,
        methods=["GET"],
        name="db_version",
    )
    app.include_router(router)


def create_app() -> FastAPI:
    app = FastAPI(title="e-Comet", lifespan=lifespan)
    register_routes(app)
    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run("task1_fastapi.app.main:create_app", factory=True)

