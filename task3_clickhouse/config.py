from pydantic import AnyUrl, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class ClickHouseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    url: AnyUrl = Field(validation_alias="CLICKHOUSE_URL")
    user: str = Field(validation_alias="CLICKHOUSE_USER")
    password: SecretStr | None = Field(default=None, validation_alias="CLICKHOUSE_PASSWORD")
    database: str = Field(validation_alias="CLICKHOUSE_DATABASE")
    repositories_table: str = Field(validation_alias="CLICKHOUSE_REPOSITORIES_TABLE")
    rankings_table: str = Field(validation_alias="CLICKHOUSE_REPOSITORY_RANKINGS_TABLE")
    author_commits_table: str = Field(validation_alias="CLICKHOUSE_REPOSITORY_AUTHORS_TABLE")
    batch_size: int = Field(default=500, validation_alias="CLICKHOUSE_INSERT_BATCH_SIZE", ge=1)
