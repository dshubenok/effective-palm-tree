from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime
from itertools import islice
from typing import AsyncIterator, Iterable, Sequence

from aiochclient import ChClient
from aiohttp import ClientSession

from task2_scraper.models import Repository

from .config import ClickHouseSettings


class ClickHouseSaverError(Exception):
    """Raised when persisting data to ClickHouse fails."""


class ClickHouseSaver:
    def __init__(self, settings: ClickHouseSettings | None = None) -> None:
        self._settings = settings or ClickHouseSettings()
        self._session: ClientSession | None = None
        self._client: ChClient | None = None
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def connect(self) -> AsyncIterator["ClickHouseSaver"]:
        await self._ensure_client()
        try:
            yield self
        finally:
            await self.close()

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _ensure_client(self) -> None:
        async with self._lock:
            if self._client is not None:
                return
            self._session = ClientSession()
            password = (
                self._settings.password.get_secret_value() if self._settings.password is not None else ""
            )
            self._client = ChClient(
                self._session,
                url=str(self._settings.url),
                user=self._settings.user,
                password=password,
                database=self._settings.database,
            )

    async def save_top_repositories(
        self,
        repositories: Sequence[Repository],
        updated_at: datetime,
    ) -> None:
        if not repositories:
            return

        await self._ensure_client()
        measurement_date = updated_at.date()

        await self._insert_repositories(repositories, updated_at)
        await self._insert_positions(repositories, measurement_date)
        await self._insert_author_commits(repositories, measurement_date)

    async def _insert_repositories(
        self,
        repositories: Sequence[Repository],
        updated_at: datetime,
    ) -> None:
        rows = (
            (
                repo.name,
                repo.owner,
                repo.stars,
                repo.watchers,
                repo.forks,
                repo.language,
                updated_at,
            )
            for repo in repositories
        )
        columns = "name, owner, stars, watchers, forks, language, updated"
        await self._insert_with_batches(self._settings.repositories_table, columns, rows)

    async def _insert_positions(
        self,
        repositories: Sequence[Repository],
        measurement_date: date,
    ) -> None:
        rows = (
            (
                measurement_date,
                self._format_repo_identifier(repo),
                repo.position if repo.position > 0 else index,
            )
            for index, repo in enumerate(repositories, start=1)
        )
        columns = "date, repo, position"
        await self._insert_with_batches(self._settings.positions_table, columns, rows)

    async def _insert_author_commits(
        self,
        repositories: Sequence[Repository],
        measurement_date: date,
    ) -> None:
        rows = (
            (
                measurement_date,
                self._format_repo_identifier(repo),
                author.author,
                author.commits_num,
            )
            for repo in repositories
            for author in repo.authors_commits_num_today
        )
        columns = "date, repo, author, commits_num"
        await self._insert_with_batches(self._settings.authors_commits_table, columns, rows)

    @staticmethod
    def _format_repo_identifier(repository: Repository) -> str:
        return f"{repository.owner}/{repository.name}"

    async def _insert_with_batches(
        self,
        table: str,
        columns: str,
        rows: Iterable[tuple],
    ) -> None:
        if self._client is None:
            raise ClickHouseSaverError("ClickHouse client is not initialized")

        batch_size = self._settings.batch_size
        iterator = iter(rows)
        while True:
            batch = list(islice(iterator, batch_size))
            if not batch:
                return
            query = f"INSERT INTO {table} ({columns}) VALUES"
            try:
                await self._client.insert(query, batch)
            except Exception as exc:  # noqa: BLE001
                raise ClickHouseSaverError(f"Failed to insert data into {table}") from exc
