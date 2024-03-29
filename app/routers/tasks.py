from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.models.file import File
from app.models.task import Task, TasksResponse
from app.models.user import User
from app.services.auth import get_current_user

router = APIRouter()


@router.get("/tasks", response_model=TasksResponse)
async def get_all_tasks(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    limit: int = 10,
    offset: int = 0,
):
    tasks = await session.scalars(
        select(Task).where(Task.user_id == current_user.id).limit(limit).offset(offset)
    )

    tasks = list(tasks)

    to_return = []

    for task in tasks:
        file = await session.scalar(select(File).where(task.file_id == File.id))

        user = await session.scalar(select(User).where(task.user_id == User.id))

        task = task.to_dict()
        task["file_name"] = file.file_name
        task["user_name"] = user.name

        to_return.append(task)

    return {"tasks": to_return, "total": len(tasks)}


@router.get("/tasks/{task_id}", response_model=TasksResponse)
async def get_task(
    task_id: UUID,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    limit: int = 10,
    offset: int = 0,
):
    tasks = await session.scalars(
        select(Task)
        .where(Task.user_id == current_user.user_id)
        .limit(limit)
        .offset(offset)
    )

    tasks = tasks.to_dict()

    to_return = []

    for task in tasks:
        file = await session.scalar(select(File).where(task["file_id"] == File.id))
        task["file_name"] = file.file_name

        user = await session.scalar(select(User).where(task["user_id"] == User.id))
        task["user_name"] = user.name

        to_return.appned(task)

    return {"tasks": to_return, "total": len(tasks)}
