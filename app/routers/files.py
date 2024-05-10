import io
from datetime import datetime, timedelta
from typing import Annotated, Literal, Union
from uuid import UUID

import boto3
import pandas as pd
import requests
from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, status
from sqlalchemy import delete, func, insert, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from thefuzz import fuzz

from app.config import get_settings
from app.db import get_async_session, get_session
from app.models.file import (
    DataQuality,
    File,
    FileResponse,
    FilesResponse,
    FileStats,
    GraphResponse,
)
from app.models.task import Task
from app.models.user import User
from app.services.auth import get_current_user

router = APIRouter()
settings = get_settings()

aws_session = boto3.Session(
    aws_access_key_id=settings.aws_access_key_id,
    aws_secret_access_key=settings.aws_access_key,
)

s3 = aws_session.resource("s3")
client = aws_session.client("s3")  # , endpoint_url="https://cdn.future-fdn.tech")
bucket = s3.Bucket(settings.aws_storage_bucket_name)


def read_file(file: File, is_csv=None) -> pd.DataFrame:
    url = client.generate_presigned_url(
        "get_object",
        ExpiresIn=3600,
        Params={
            "Bucket": settings.aws_storage_bucket_name,
            "Key": file.type.title() + "/" + file.file_name
            if file.type.lower() != "result"
            else file.type.lower() + "/" + file.file_name,
        },
    ).replace("s3.amazonaws.com/", "")

    if file.file_name.endswith(".csv") or is_csv:
        response = requests.get(url)

        df = pd.read_csv(io.StringIO(response.content.decode("utf-8")))
    elif file.file_name.endswith(".txt"):
        response = requests.get(url)
        df = pd.read_fwf(io.StringIO(response.content.decode("utf-8")), header=None)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File type not supported",
        )

    return df


@router.get(
    "/files/{file_id}",
    response_model=Union[FilesResponse | FileResponse | FileStats | GraphResponse],
)
async def get_specific_files(
    file_id: UUID | Literal["master", "query", "stats", "graph"],
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    limit: int = 10,
    offset: int = 0,
):
    files = []
    url = None

    if file_id == "stats":
        date = datetime.now().replace(day=1).date()
        date_minus_one = (datetime.now() - timedelta(days=datetime.now().day)).replace(
            day=1
        )

        info = await session.scalar(select(DataQuality).where(DataQuality.date == date))
        query_count = await session.scalar(
            select(func.count())
            .select_from(File)
            .where(File.user_id == current_user.id)
            .where(File.type == "QUERY")
            .where(
                File.created
                >= datetime.now().replace(
                    day=1, hour=0, minute=0, second=0, microsecond=0
                )
            )
        )

        if not info:
            info = await session.scalar(
                select(DataQuality).where(
                    DataQuality.date
                    == (date_minus_one - timedelta(days=date_minus_one.day))
                    .replace(day=1)
                    .date()
                )
            )
            if not info:
                return_values = {
                    "overall_uniqueness": format(0.0, "00.0f") + "%",
                    "overall_completeness": format(0.0, "00.0f") + "%",
                    "total_query_records": 0,
                    "total_master_records": 0,
                    "uniqueness_diff": format(
                        0.0,
                        "00.0f",
                    )
                    + "%",
                    "completeness_diff": format(
                        0.0,
                        "00.0f",
                    )
                    + "%",
                    "query_records_diff": 0,
                    "master_records_diff": 0,
                    "this_month_query_data": query_count,
                }

                return_values.update(
                    {
                        "uniqueness_diff": "+" + return_values["uniqueness_diff"]
                        if not return_values["uniqueness_diff"].startswith("-")
                        else return_values["uniqueness_diff"],
                        "completeness_diff": "+" + return_values["completeness_diff"]
                        if not return_values["uniqueness_diff"].startswith("-")
                        else return_values["uniqueness_diff"],
                    }
                )

                return return_values

        return_values = {
            "overall_uniqueness": format((info.overall_uniqueness) * 100.0, "00.0f")
            + "%",
            "overall_completeness": format((info.overall_completeness) * 100.0, "00.0f")
            + "%",
            "total_query_records": info.total_query_records,
            "total_master_records": info.total_master_records,
        }

        info_previous = await session.scalar(
            select(DataQuality).where(DataQuality.date == date_minus_one.date())
        )

        if info_previous:
            return_values.update(
                {
                    "uniqueness_diff": format(
                        (info.overall_uniqueness - info_previous.overall_uniqueness)
                        * 100.0,
                        "00.0f",
                    )
                    + "%",
                    "completeness_diff": format(
                        (info.overall_completeness - info_previous.overall_completeness)
                        * 100,
                        "00.0f",
                    )
                    + "%",
                    "query_records_diff": info.total_query_records
                    - info_previous.total_query_records,
                    "master_records_diff": info.total_master_records
                    - info_previous.total_master_records,
                }
            )
        else:
            return_values.update(
                {
                    "uniqueness_diff": "100%",
                    "completeness_diff": "100%",
                    "query_records_diff": info.total_query_records,
                    "master_records_diff": info.total_master_records,
                }
            )

        return_values.update(
            {
                "uniqueness_diff": "+" + return_values["uniqueness_diff"]
                if not return_values["uniqueness_diff"].startswith("-")
                else return_values["uniqueness_diff"],
                "completeness_diff": "+" + return_values["completeness_diff"]
                if not return_values["uniqueness_diff"].startswith("-")
                else return_values["uniqueness_diff"],
                "this_month_query_data": query_count,
            }
        )
        return return_values

    elif file_id == "graph":
        date = datetime.now() - timedelta(days=365)
        date = date.replace(day=1)
        date_list = [date]

        for i in range(12):
            date_list.append((date_list[-1] + timedelta(days=31)).replace(day=1))

        info = await session.scalars(
            select(DataQuality).where(
                or_(*[DataQuality.date == x.date() for x in date_list])
            )
        )
        info = list(info)
        return_list = []

        for d in date_list:
            data = None
            for i in info:
                if d.year == i.date.year and d.month == i.date.month:
                    data = float(i.overall_completeness) * 100
                    break

            if not data:
                data = 0.0

            return_list.append({"date": str(d.date()), "value": str(int(data))})

        return {"datas": return_list}

    elif file_id == "master":
        all_files = await session.scalars(
            select(File).where(File.type == "MASTER").limit(limit).offset(offset)
        )

        count = await session.scalar(
            select(func.count()).select_from(File).where(File.type == "MASTER")
        )
    elif file_id == "query":
        all_files = await session.scalars(
            select(File)
            .where(File.user_id == current_user.id)
            .where(File.type == "QUERY")
            .limit(limit)
            .offset(offset)
        )

        count = await session.scalar(
            select(func.count())
            .select_from(File)
            .where(File.user_id == current_user.id)
            .where(File.type == "QUERY")
        )

    elif not file_id:
        if current_user.role != "ADMIN":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden"
            )

        all_files = await session.scalars(
            select(File)
            .where(or_(File.type == "MASTER", File.type == "QUERY"))
            .limit(limit)
            .offset(offset)
        )
    else:
        file = await session.scalar(
            select(File).where(File.id == file_id).limit(limit).offset(offset)
        )

        count = await session.scalar(
            select(func.count()).select_from(File).where(File.id == file_id)
        )

        if current_user.id != file.user_id and current_user.role != "ADMIN":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden"
            )

        url = client.generate_presigned_url(
            "get_object",
            ExpiresIn=3600,
            Params={
                "Bucket": settings.aws_storage_bucket_name,
                "Key": file.type.title() + "/" + file.file_name,
            },
        ).replace("s3.amazonaws.com/", "")

        file = file.to_dict()
        user = await session.scalar(select(User).where(User.id == file["user_id"]))

        file["name"] = user.name
        file["file_name"] = file["file_name"].replace(file["id"] + "_", "")

        file["url"] = url

        return file

    for file in all_files:
        url = client.generate_presigned_url(
            "get_object",
            ExpiresIn=3600,
            Params={
                "Bucket": settings.aws_storage_bucket_name,
                "Key": file.type.title() + "/" + file.file_name,
            },
        ).replace("s3.amazonaws.com/", "")

        file = file.to_dict()
        user = await session.scalar(select(User).where(User.id == file["user_id"]))

        file["name"] = user.name
        file["file_name"] = file["file_name"].replace(file["id"] + "_", "")

        files.append(
            {
                **file,
                "url": url,
            }
        )

    return {"files": files, "total": count}


@router.delete("/files/{file_id}")
async def delete_specific_files(
    file_id: UUID | Literal["master", "query"],
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    if file_id == "master" or file_id == "query":
        raise HTTPException(
            status_code=400,
            detail="Cannot delete files",
        )
    else:
        file = await session.scalar(
            delete(File).where(File.id == file_id).returning(File)
        )

        if not file:
            raise HTTPException(
                status_code=404,
                detail="File not found",
            )

        if file.type == "MASTER" and current_user.role != "ADMIN":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Not allowed"
            )

        s3.meta.client.delete_object(
            Bucket=settings.aws_storage_bucket_name,
            Key=f"{file.type.title()}/{file.file_name}",
        )
        s3.meta.client.delete_object(
            Bucket=settings.aws_storage_bucket_name,
            Key=f"{file.type.title()}/{file.id}_{file.file_name}",
        )

        await session.commit()

    return {"detail": "Deleted successfully"}


@router.get("/files", response_model=FilesResponse)
async def get_all_files(
    session: AsyncSession = Depends(get_session),
    limit: int = 10,
    offset: int = 0,
):
    files = []
    url = None

    all_files = await session.scalars(
        select(File)
        .where(or_(File.type == "MASTER", File.type == "QUERY"))
        .limit(limit)
        .offset(offset)
    )
    count = await session.scalar(
        select(func.count())
        .select_from(File)
        .where(or_(File.type == "MASTER", File.type == "QUERY"))
    )

    for file in all_files:
        url = client.generate_presigned_url(
            "get_object",
            ExpiresIn=3600,
            Params={
                "Bucket": settings.aws_storage_bucket_name,
                "Key": file.type.title() + "/" + file.file_name,
            },
        ).replace("s3.amazonaws.com/", "")

        file = file.to_dict()
        user = await session.scalar(select(User).where(User.id == file["user_id"]))

        file["name"] = user.name
        file["file_name"] = file["file_name"].replace(file["id"] + "_", "")

        files.append(
            {
                **file,
                "url": url,
            }
        )

    return {"files": files, "total": count}


@router.get("/files/{file_id}/columns")
async def list_columns(
    file_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    file = await session.scalar(select(File).where(File.id == file_id))

    if not file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found",
        )

    if file.type == "QUERY" and file.user_id != current_user.id:
        if current_user.role != "ADMIN":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You cannot access this file",
            )

    df = read_file(file)

    return {"columns": list(df.columns)}


@router.post("/files/{file_id}/map")
async def map_file(
    file_id: str,
    master_file_id: Annotated[UUID, Form()],
    query_column: Annotated[str, Form()],
    master_column: Annotated[str, Form()],
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    file = await session.scalar(select(File).where(File.id == file_id))
    master_file = await session.scalar(select(File).where(File.id == master_file_id))

    if not file or not master_file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found",
        )

    if file.type == "MASTER" or master_file.type == "QUERY":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot map master file",
        )

    df = read_file(file)
    master_df = read_file(master_file)

    task = await session.scalar(
        insert(Task)
        .values(file_id=file.id, user_id=current_user.id, status="PENDING")
        .returning(Task)
    )

    if not task:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="There was an error creating the task",
        )

    await session.commit()

    background_tasks.add_task(
        map_data, df, master_df, query_column, master_column, file, current_user
    )

    return {"detail": "Added to tasks successfully"}


async def map_data(
    df: pd.DataFrame, master_df, query_column, master_column, file, current_user
):
    session = get_async_session()

    if not master_df.columns.isin(
        [master_column if not master_column.isnumeric() else int(master_column)]
    ).any():
        await session.scalar(
            update(Task)
            .where(Task.file_id == file.id)
            .values(status="FAILED")
            .returning(Task)
        )

        return

    if not df.columns.isin(
        [query_column if not query_column.isnumeric() else int(query_column)]
    ).any():
        await session.scalar(
            update(Task)
            .where(Task.file_id == file.id)
            .values(status="FAILED", ended=datetime.now())
            .returning(Task)
        )

        return

    data = []

    for i, row in enumerate(
        df[query_column if not query_column.isnumeric() else int(query_column)]
    ):
        partial = master_df[
            master_column if not master_column.isnumeric() else int(master_column)
        ].apply(lambda x: (fuzz.partial_ratio(x, row)))
        full = master_df[
            master_column if not master_column.isnumeric() else int(master_column)
        ].apply(lambda x: (fuzz.ratio(x, row)))

        if full.max() > 90:
            data.append(
                [
                    df[
                        query_column
                        if not query_column.isnumeric()
                        else int(query_column)
                    ][i],
                    master_df[
                        master_column
                        if not master_column.isnumeric()
                        else int(master_column)
                    ][partial.idxmax()],
                    partial.max(),
                    full.max(),
                ]
            )
        elif partial.max() > 90:
            data.append(
                [
                    df[
                        query_column
                        if not query_column.isnumeric()
                        else int(query_column)
                    ][i],
                    master_df[
                        master_column
                        if not master_column.isnumeric()
                        else int(master_column)
                    ][partial.idxmax()],
                    partial.max(),
                    full.max(),
                ]
            )
        else:
            data.append(
                [
                    df[
                        query_column
                        if not query_column.isnumeric()
                        else int(query_column)
                    ][i],
                    master_df[
                        master_column
                        if not master_column.isnumeric()
                        else int(master_column)
                    ][partial.idxmax()],
                    partial.max(),
                    full.max(),
                ]
            )

    resulting_df = pd.DataFrame(
        data, columns=["source", "destination", "partial", "full"]
    )
    buffer = io.BytesIO(resulting_df.to_csv(index=False).encode())
    client.upload_fileobj(
        buffer,
        settings.aws_storage_bucket_name,
        "result" + "/" + f"{file.id}_{file.file_name}",
    )

    result_file = await session.scalar(
        insert(File)
        .values(
            file_name=f"{file.id}_{file.file_name}",
            user_id=current_user.id,
            description="",
            unique=0,
            valid=0,
            total=0,
            type="RESULT",
        )
        .returning(File)
    )
    await session.commit()

    await session.scalar(
        update(Task)
        .where(Task.file_id == file.id)
        .values(ended=datetime.now(), status="COMPLETED", file_id=result_file.id)
        .returning(Task)
    )

    await session.commit()

    del df
    del master_df


@router.post(
    "/files",
    status_code=status.HTTP_201_CREATED,
)
async def create_file_object(
    file_name: Annotated[str, Form()],
    file_type: Annotated[Literal["QUERY", "MASTER"], Form()],
    description: Annotated[str, Form()] = "",
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    if file_type == "MASTER" and current_user.role != "ADMIN":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not allowed")

    file = await session.scalar(
        insert(File)
        .values(
            file_name=file_name,
            user_id=current_user.id,
            description=description,
            unique=0,
            valid=0,
            total=0,
            type=file_type,
        )
        .returning(File)
    )
    file = await session.scalar(
        update(File)
        .where(File.id == file.id)
        .values(file_name=f"{file.id}_{file_name}")
        .returning(File)
    )

    await session.commit()

    upload_detail = client.generate_presigned_post(
        settings.aws_storage_bucket_name,
        file_type.title() + "/" + file.file_name,
    )
    upload_detail.update({"url": "https://storage.future-fdn.tech"})

    return {"upload_detail": upload_detail, "file_id": file.id}


@router.patch(
    "/files/{file_id}",
    status_code=status.HTTP_201_CREATED,
)
async def patch_file(
    file_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    file = await session.scalar(select(File).where(File.id == file_id))

    if file.type == "MASTER" and current_user.role != "ADMIN":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not allowed")

    url = client.generate_presigned_url(
        "get_object",
        ExpiresIn=3600,
        Params={
            "Bucket": settings.aws_storage_bucket_name,
            "Key": file.type.title() + "/" + file.file_name,
        },
    ).replace("s3.amazonaws.com/", "")

    if file.file_name.endswith(".csv"):
        response = requests.get(url)

        df = pd.read_csv(
            io.StringIO(response.content.decode("utf-8")), on_bad_lines="skip"
        )
    elif file.file_name.endswith(".txt"):
        response = requests.get(url)
        df = pd.read_fwf(
            io.StringIO(response.content.decode("utf-8")),
            header=None,
            on_bad_lines="skip",
        )

    total_unique_values = df.nunique().sum()
    total_non_na = df.count().sum()
    total = df.size

    await session.scalar(
        update(File)
        .values(unique=total_unique_values, valid=total_non_na, total=total)
        .where(File.id == file.id)
        .returning(File)
    )

    await session.commit()

    return {"detail": "Success!"}
