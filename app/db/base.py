from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from app.db import engine


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with engine.get_async_session() as session:
        yield session
