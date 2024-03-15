from uuid import UUID

from sqlalchemy import delete, insert, select

from app.db import engine
from app.users.models import User


async def get_user(user_id: UUID) -> User:
    async with engine.get_async_session() as session:
        user = await session.scalar(select(User).where(user_id == User.id))

    return user


async def delete_user(user_id: UUID) -> User:
    async with engine.get_async_session() as session:
        user = await session.scalar(delete(User).where(user_id == User.id))
        await session.commit()

    return user


async def get_users() -> User:
    async with engine.get_async_session() as session:
        user = await session.scalars(select(User))

    return user


async def get_user_by_email(email: str) -> User:
    async with engine.get_async_session() as session:
        user = await session.scalar(select(User).where(email == User.email))

    return user


async def add_new_user(user: User) -> User:
    async with engine.get_async_session() as session:
        user = await session.scalar(
            insert(User)
            .values(
                name=user.name, email=user.email, password=user.password, role=user.role
            )
            .returning(User)
        )
        await session.commit()

    return user
