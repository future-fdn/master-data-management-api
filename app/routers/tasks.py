import io
import re
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Form, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import get_session
from app.models.file import File
from app.models.task import Task, TaskResponse, TasksResponse
from app.models.user import User
from app.routers.files import client, read_file, s3
from app.services.auth import get_current_user

router = APIRouter()


settings = get_settings()


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
        task["file_name"] = re.sub(
            r"[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}_",
            "",
            file.file_name,
        )
        task["user_name"] = user.name

        task["url"] = client.generate_presigned_url(
            "get_object",
            ExpiresIn=3600,
            Params={
                "Bucket": settings.aws_storage_bucket_name,
                "Key": file.type.title()
                if file.type.lower() != "result"
                else file.type.lower() + "/" + file.file_name,
            },
        ).replace("s3.amazonaws.com/", "")

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

    if current_user.id != task.user_id and current_user.role != "ADMIN":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)

    file = await session.scalar(select(File).where(File.id == Task.file_id))

    if not file:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    task["url"] = client.generate_presigned_url(
        "get_object",
        ExpiresIn=3600,
        Params={
            "Bucket": settings.aws_storage_bucket_name,
            "Key": file.type.title()
            if file.type.lower() != "result"
            else file.type.lower() + "/" + file.file_name,
        },
    ).replace("s3.amazonaws.com/", "")
    task["file_name"] = file.name
    task["user_name"] = current_user.name

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
        Bucket=settings.aws_storage_bucket_name,
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


@router.get("/tasks/{task_id}/table")
async def get_data_table(
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

    return data.to_dict(orient="records")


@router.put("/tasks/{task_id}")
async def edit_task(
    task_id: UUID,
    source: Annotated[str, Form()],
    destination: Annotated[str, Form()],
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

    df = read_file(file, is_csv=True)

    df.loc[
        (df["source"] == source),
        ["full", "partial"],
    ] = 100

    df.loc[
        (df["source"] == source),
        ["destination"],
    ] = destination

    buffer = io.BytesIO(df.to_csv(index=False).encode())
    client.upload_fileobj(
        buffer,
        settings.aws_storage_bucket_name,
        "result" + "/" + f"{file.file_name}",
    )

    return {"detail": "Success!"}


@router.patch("/tasks/{task_id}/versions")
async def revert_version(
    task_id: UUID,
    version_id: Annotated[str, Form()],
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

    versions = s3.meta.client.list_object_versions(
        Bucket=settings.aws_storage_bucket_name, Prefix=f"result/{file.file_name}"
    ).get("Versions", [])

    s3.meta.client.copy(
        {
            "Bucket": settings.aws_storage_bucket_name,
            "Key": versions[0]["Key"],
            "VersionId": version_id,
        },
        settings.aws_storage_bucket_name,
        versions[0]["Key"],
    )

    s3.meta.client.delete_object(
        Bucket=settings.aws_storage_bucket_name,
        Key=f"result/{file.file_name}",
        VersionId=version_id,
    )

    return {"detail": "Success!"}
