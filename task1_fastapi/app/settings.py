from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    host: str = Field(validation_alias="DB_HOST")
    port: int = Field(default=5432, validation_alias="DB_PORT", ge=0, le=65535)
    user: str = Field(validation_alias="DB_USER")
    password: SecretStr = Field(validation_alias="DB_PASSWORD")
    database: str = Field(validation_alias="DB_NAME")
    min_size: int = Field(default=1, validation_alias="DB_POOL_MIN_SIZE", ge=1)
    max_size: int = Field(default=10, validation_alias="DB_POOL_MAX_SIZE", ge=1)
    command_timeout: float = Field(default=30.0, validation_alias="DB_COMMAND_TIMEOUT", gt=0.0)

    @model_validator(mode="after")
    def validate_pool_size(self) -> "PostgresSettings":
        if self.min_size > self.max_size:
            raise ValueError("DB_POOL_MIN_SIZE cannot exceed DB_POOL_MAX_SIZE")
        return self
