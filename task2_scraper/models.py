from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True, slots=True)
class AuthorCommits:
    author: str
    commits: int


@dataclass(slots=True)
class Repository:
    name: str
    full_name: str
    html_url: str
    stargazers_count: int
    forks_count: int
    authors_commits_num_today: Sequence[AuthorCommits]
