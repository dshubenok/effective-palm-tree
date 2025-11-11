from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from itertools import islice
from typing import AsyncIterator, Iterable, Sequence

from aiochclient import ChClient
from aiohttp import ClientSession

from task2_scraper.models import Repository

from .config import ClickHouseSettings


class ClickHouseSaverError(Exception):
    """Raised when persisting data to ClickHouse fails."""


@dataclass(slots=True)
class RepositorySnapshot:
    measured_at: datetime
    repository: Repository
    position: int


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
        measured_at: datetime,
    ) -> None:
        if not repositories:
            return

        await self._ensure_client()
        snapshots = [
            RepositorySnapshot(
                measured_at=measured_at,
                repository=repo,
                position=index,
            )
            for index, repo in enumerate(repositories, start=1)
        ]

        await self._insert_repositories(snapshots)
        await self._insert_rankings(snapshots)
        await self._insert_author_commits(snapshots)

    async def _insert_repositories(self, snapshots: Sequence[RepositorySnapshot]) -> None:
        rows = (
            (
                snapshot.measured_at,
                snapshot.repository.full_name,
                snapshot.repository.name,
                snapshot.repository.html_url,
                snapshot.repository.stargazers_count,
                snapshot.repository.forks_count,
            )
            for snapshot in snapshots
        )
        columns = "measured_at, full_name, name, html_url, stargazers_count, forks_count"
        await self._insert_with_batches(self._settings.repositories_table, columns, rows)

    async def _insert_rankings(self, snapshots: Sequence[RepositorySnapshot]) -> None:
        rows = (
            (
                snapshot.measured_at,
                snapshot.repository.full_name,
                snapshot.position,
            )
            for snapshot in snapshots
        )
        columns = "measured_at, full_name, position"
        await self._insert_with_batches(self._settings.rankings_table, columns, rows)

    async def _insert_author_commits(self, snapshots: Sequence[RepositorySnapshot]) -> None:
        rows = (
            (
                snapshot.measured_at,
                snapshot.repository.full_name,
                author.author,
                author.commits,
            )
            for snapshot in snapshots
            for author in snapshot.repository.authors_commits_num_today
        )
        columns = "measured_at, full_name, author, commits"
        await self._insert_with_batches(self._settings.author_commits_table, columns, rows)

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
