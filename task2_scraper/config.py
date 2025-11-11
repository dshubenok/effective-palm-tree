from pydantic import AnyHttpUrl, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class GithubSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    api_base_url: AnyHttpUrl = Field(
        default="https://api.github.com",
        validation_alias="GITHUB_API_BASE_URL",
    )
    token: SecretStr | None = Field(default=None, validation_alias="GITHUB_TOKEN")
    timeout: float = Field(default=10.0, validation_alias="GITHUB_TIMEOUT_SECONDS", gt=0.0)
    max_concurrent_requests: int = Field(
        default=5,
        validation_alias="GITHUB_MAX_CONCURRENT_REQUESTS",
        ge=1,
    )
    requests_per_second: int = Field(
        default=10,
        validation_alias="GITHUB_REQUESTS_PER_SECOND",
        ge=1,
    )
    commits_page_size: int = Field(
        default=100,
        validation_alias="GITHUB_COMMITS_PAGE_SIZE",
        ge=1,
        le=100,
    )
