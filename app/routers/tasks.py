from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.models.file import File
from app.models.task import Task, TaskResponse, TasksResponse
from app.models.user import User
from app.routers.files import client, read_file
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
    count = session.scalar(
        select(func.count()).select_from(Task).where(Task.user_id == current_user.id)
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

    return {"tasks": to_return, "total": await count}


@router.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: UUID,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    limit: int = 10,
    offset: int = 0,
):
    task = await session.scalar(
        select(Task).where(Task.id == task_id).limit(limit).offset(offset)
    )

    return task


@router.get("/tasks/{task_id}/versions")
async def get_versions(
    task_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    task = await session.scalar(select(Task).where(Task.id == task_id))

    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found",
        )

    file = await session.scalar(select(File).where(File.id == task.file_id))

    if not file:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="File not found",
        )

    versions = client.list_object_versions(
        Bucket="storage.future-fdn.tech",
        Prefix="result" + "/" + file.file_name,
    )

    version_return = [
        {
            "id": x["VersionId"],
            "latest": x["IsLatest"],
            "modified": x["LastModified"],
            "key": x["Key"],
        }
        for x in versions["Versions"]
    ]

    return {"versions": version_return}


@router.get("/tasks/{task_id}/data")
async def get_data(
    task_id: UUID,
    session: AsyncSession = Depends(get_session),
):
    task = await session.scalar(select(Task).where(Task.id == task_id))

    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found",
        )

    file = await session.scalar(select(File).where(File.id == task.file_id))

    if not file:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="File not found",
        )

    data = read_file(file, is_csv=True)
    data_json = data.to_dict(orient="records")
    nodes = set([x["source"] for x in data_json])
    node_dest = set([x["destination"] for x in data_json])
    nodes = [{"id": x, "group": 1} for x in nodes]
    nodes.extend([{"id": x, "group": 2} for x in node_dest])
    links = []

    for link in data_json:
        jsn = {}
        for k, v in link.items():
            if k == "destination":
                k = "target"

            if k == "full":
                k = "distance"

            jsn.update({k: v})

        links.append(jsn)

    return {
        "nodes": nodes,
        "links": links,
    }
