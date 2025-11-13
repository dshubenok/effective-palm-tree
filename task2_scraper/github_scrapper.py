from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Final

from aiohttp import ClientError, ClientResponseError, ClientSession

from .models import Repository, RepositoryAuthorCommitsNum
from .rate_limiter import RateLimiter

GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"


class GithubReposScrapper:
    def __init__(
        self,
        access_token: str,
        *,
        max_concurrent_requests: int = 5,
        requests_per_second: int = 10,
    ) -> None:
        if max_concurrent_requests <= 0:
            raise ValueError("max_concurrent_requests must be positive")
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"Bearer {access_token}",
            "User-Agent": "effective-palm-tree-scrapper",
        }
        self._session = ClientSession(headers=headers)
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._rate_limiter = RateLimiter(requests_per_second)

    async def __aenter__(self) -> "GithubReposScrapper":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def close(self) -> None:
        if not self._session.closed:
            await self._session.close()

    async def get_repositories(self, limit: int = 100) -> list[Repository]:
        repositories = await self._get_top_repositories(limit=limit)
        since = datetime.now(timezone.utc) - timedelta(days=1)

        tasks = [
            self._build_repository_snapshot(position, repo, since)
            for position, repo in enumerate(repositories, start=1)
        ]
        if not tasks:
            return []
        return await asyncio.gather(*tasks)

    async def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{GITHUB_API_BASE_URL}/{endpoint}"
        try:
            async with self._semaphore:
                await self._rate_limiter.acquire()
                async with self._session.request(method, url, params=params) as response:
                    response.raise_for_status()
                    return await response.json()
        except ClientResponseError as exc:
            text = exc.message or "GitHub API request failed"
            raise RuntimeError(f"{method} {url} failed with status {exc.status}: {text}") from exc
        except ClientError as exc:
            raise RuntimeError(f"Network error during {method} {url}") from exc

    async def _get_top_repositories(self, limit: int = 100) -> list[dict[str, Any]]:
        data = await self._make_request(
            endpoint="search/repositories",
            params={"q": "stars:>1", "sort": "stars", "order": "desc", "per_page": limit},
        )
        items = data.get("items")
        if not isinstance(items, list):
            raise RuntimeError("GitHub API returned unexpected payload for repositories search")
        return items

    async def _get_repository_commits(
        self,
        owner: str,
        repo: str,
        since: datetime,
    ) -> list[dict[str, Any]]:
        params = {
            "since": since.isoformat(timespec="seconds"),
            "per_page": 100,
        }
        data = await self._make_request(
            endpoint=f"repos/{owner}/{repo}/commits",
            params=params,
        )
        if not isinstance(data, list):
            raise RuntimeError("GitHub API returned unexpected payload for repository commits")
        return data

    async def _build_repository_snapshot(
        self,
        position: int,
        repo: dict[str, Any],
        since: datetime,
    ) -> Repository:
        owner = repo["owner"]["login"]
        name = repo["name"]
        commits = await self._get_repository_commits(owner=owner, repo=name, since=since)
        authors = self._aggregate_commits(commits)
        return Repository(
            name=name,
            owner=owner,
            position=position,
            stars=repo.get("stargazers_count", 0),
            watchers=repo.get("watchers_count") or repo.get("watchers", 0),
            forks=repo.get("forks_count", 0),
            language=repo.get("language") or "",
            authors_commits_num_today=authors,
        )

    @staticmethod
    def _aggregate_commits(commits: list[dict[str, Any]]) -> list[RepositoryAuthorCommitsNum]:
        authors = Counter()
        for commit in commits:
            author = GithubReposScrapper._extract_author(commit)
            if author:
                authors[author] += 1
        return [
            RepositoryAuthorCommitsNum(author=author, commits_num=count)
            for author, count in authors.most_common()
        ]

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
        return commit_author.get("email")
