from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Sequence

import httpx

from .config import GithubSettings
from .models import AuthorCommits, Repository
from .rate_limiter import RateLimiter


class GithubScrapperError(RuntimeError):
    """Base exception for GithubReposScrapper errors."""


class GithubReposScrapper:
    def __init__(self, settings: GithubSettings | None = None) -> None:
        self._settings = settings or GithubSettings()
        self._semaphore = asyncio.Semaphore(self._settings.max_concurrent_requests)
        self._rate_limiter = RateLimiter(self._settings.requests_per_second)
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "effective-palm-tree-scrapper",
        }
        if self._settings.token:
            headers["Authorization"] = f"Bearer {self._settings.token.get_secret_value()}"
        self._client = httpx.AsyncClient(
            base_url=str(self._settings.api_base_url),
            headers=headers,
            timeout=httpx.Timeout(self._settings.timeout),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "GithubReposScrapper":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def get_repositories(self, repositories: Sequence[str]) -> list[Repository]:
        tasks = [self._fetch_repository(name) for name in repositories]
        if not tasks:
            return []
        return await asyncio.gather(*tasks)

    async def _fetch_repository(self, repo_full_name: str) -> Repository:
        repo_data = await self._get_json(f"/repos/{repo_full_name}")
        authors = await self._fetch_authors_commits(repo_full_name)
        return Repository(
            name=repo_data["name"],
            full_name=repo_data["full_name"],
            html_url=repo_data["html_url"],
            stargazers_count=repo_data.get("stargazers_count", 0),
            forks_count=repo_data.get("forks_count", 0),
            authors_commits_num_today=authors,
        )

    async def _fetch_authors_commits(self, repo_full_name: str) -> list[AuthorCommits]:
        since = datetime.now(timezone.utc) - timedelta(days=1)
        params = {"since": since.isoformat(timespec="seconds"), "per_page": self._settings.commits_page_size}

        authors: dict[str, int] = {}
        async for commit in self._iterate_commits(repo_full_name, params):
            author_login = self._extract_author(commit)
            if not author_login:
                continue
            authors[author_login] = authors.get(author_login, 0) + 1

        return [AuthorCommits(author=author, commits=count) for author, count in authors.items()]

    async def _iterate_commits(self, repo_full_name: str, params: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
        next_url: str | None = f"/repos/{repo_full_name}/commits"
        next_params = params
        while next_url:
            response = await self._request(next_url, params=next_params)
            data = response.json()
            for item in data:
                yield item

            link = response.links.get("next")
            next_url = link["url"] if link else None
            next_params = None

    async def _request(self, url: str, params: dict[str, Any] | None = None) -> httpx.Response:
        async with self._semaphore:
            await self._rate_limiter.acquire()
            try:
                response = await self._client.get(url, params=params)
            except httpx.HTTPError as exc:
                raise GithubScrapperError(f"HTTP error while requesting {url}") from exc

        if response.status_code == httpx.codes.OK:
            return response
        if response.status_code == httpx.codes.NOT_FOUND:
            raise GithubScrapperError(f"Resource not found: {url}")
        if response.status_code == httpx.codes.FORBIDDEN:
            raise GithubScrapperError("Access forbidden by GitHub API")
        if response.status_code == httpx.codes.UNPROCESSABLE_ENTITY:
            raise GithubScrapperError(f"Unprocessable entity for request {url}")

        raise GithubScrapperError(
            f"Unexpected status {response.status_code} for request {url}: {response.text}"
        )

    async def _get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        return (await self._request(url, params=params)).json()

    @staticmethod
    def _extract_author(commit: dict[str, Any]) -> str | None:
        author = commit.get("author") or {}
        login = author.get("login")
        if login:
            return login

        commit_author = commit.get("commit", {}).get("author", {})
        name = commit_author.get("name")
        if name:
            return name
        email = commit_author.get("email")
        return email
