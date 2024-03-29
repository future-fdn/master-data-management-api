from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    postgres_server: str
    postgres_port: int
    postgres_user: str
    postgres_db: str
    postgres_password: str

    aws_access_key_id: str
    aws_access_key: str

    secret: str

    origin: list

    model_config = SettingsConfigDict(env_file=".env")


@lru_cache
def get_settings() -> Settings:
    return Settings()
