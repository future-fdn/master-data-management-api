from sqlalchemy.engine.url import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings


def new_async_engine(uri: URL) -> AsyncEngine:
    return create_async_engine(
        uri,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30.0,
        pool_recycle=600,
    )


_ASYNC_ENGINE = new_async_engine(settings.DB_URI)
_ASYNC_SESSIONMAKER = async_sessionmaker(_ASYNC_ENGINE, expire_on_commit=False)


def get_async_session() -> AsyncSession:  # pragma: no cover
    return _ASYNC_SESSIONMAKER()
