from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class RepositoryAuthorCommitsNum:
    author: str
    commits_num: int


@dataclass(slots=True)
class Repository:
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]
