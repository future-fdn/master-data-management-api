import io
from datetime import datetime
from typing import Annotated, Literal, Union
from uuid import UUID

import boto3
import pandas as pd
import requests
from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, status
from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from thefuzz import fuzz

from app.config import get_settings
from app.db import get_async_session, get_session
from app.models.file import File, FileResponse, FilesResponse
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
bucket = s3.Bucket("cdn.future-fdn.tech")


@router.get("/files/{file_id}", response_model=Union[FilesResponse | FileResponse])
async def get_specific_files(
    file_id: UUID | Literal["master", "query"],
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    limit: int = 10,
    offset: int = 0,
):
    files = []
    url = None

    if file_id == "master":
        if current_user.role != "ADMIN":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden"
            )

        all_files = await session.scalars(
            select(File).where(File.type == "MASTER").limit(limit).offset(offset)
        )
    elif file_id == "query":
        all_files = await session.scalars(
            select(File)
            .where(File.user_id == current_user.id)
            .where(File.type == "QUERY")
            .limit(limit)
            .offset(offset)
        )
    elif not file_id:
        if current_user.role != "ADMIN":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden"
            )

        all_files = await session.scalars(select(File).limit(limit).offset(offset))
    else:
        file = await session.scalar(
            select(File).where(File.id == file_id).limit(limit).offset(offset)
        )

        if current_user.id != file.user_id and current_user.role != "ADMIN":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden"
            )

        obj = [
            x
            for x in bucket.objects.filter(
                Prefix=file.type.title() + "/" + file.file_name
            )
        ][0]

        url = client.generate_presigned_url(
            "get_object",
            ExpiresIn=3600,
            Params={"Bucket": "cdn.future-fdn.tech", "Key": obj.key},
        ).replace("s3.amazonaws.com/", "")

        # versions = client.list_object_versions(
        #     Bucket="cdn.future-fdn.tech", Prefix=file.type.title() + "/" + file.file_name
        # )

        file = file.to_dict()
        user = await session.scalar(select(User).where(User.id == file["user_id"]))

        file["name"] = user.name

        file["url"] = url
        # file["versions"] = [
        #     {
        #         "id": "",
        #         "latest": x["IsLatest"],
        #         "modified": x["LastModified"],
        #         "key": x["Key"],
        #     }
        #     for x in versions["Versions"]
        # ]

        return file

    for file in all_files:
        obj = list(
            bucket.objects.filter(Prefix=file.type.title() + "/" + file.file_name)
        )[0]

        if not obj:
            continue

        url = client.generate_presigned_url(
            "get_object",
            ExpiresIn=3600,
            Params={"Bucket": "cdn.future-fdn.tech", "Key": obj.key},
        ).replace("s3.amazonaws.com/", "")

        # versions = client.list_object_versions(
        #     Bucket="cdn.future-fdn.tech", Prefix=file.type.title() + "/" + file.file_name
        # )

        file = file.to_dict()
        user = await session.scalar(select(User).where(User.id == file["user_id"]))

        file["name"] = user.name
        # file["versions"] = [
        #     {
        #         "id": x["VersionId"],
        #         "latest": x["IsLatest"],
        #         "modified": x["LastModified"],
        #         "key": x["Key"],
        #     }
        #     for x in versions["Versions"]
        # ]

        files.append(
            {
                **file,
                "url": url,
            }
        )

    return {"files": files, "total": len(files)}


@router.delete("/files/{file_id}")
async def delete_specific_files(
    file_id: UUID | Literal["master", "query"],
    session: AsyncSession = Depends(get_session),
):
    if file_id == "master":
        raise HTTPException(
            status_code=400,
            detail="Cannot delete master files",
        )
    elif file_id == "query":
        raise HTTPException(
            status_code=400,
            detail="Cannot delete query files",
        )
    else:
        file = await session.scalar(
            delete(File).where(File.id == file_id).returning(File)
        )
        await session.commit()

        if not file:
            raise HTTPException(
                status_code=404,
                detail="File not found",
            )

    return {"detail": "Deleted successfully"}


@router.get("/files", response_model=FilesResponse)
async def get_all_files(
    session: AsyncSession = Depends(get_session),
    limit: int = 10,
    offset: int = 0,
):
    files = []
    url = None

    all_files = await session.scalars(select(File).limit(limit).offset(offset))

    for file in all_files:
        obj = list(
            bucket.objects.filter(Prefix=file.type.title() + "/" + file.file_name)
        )[0]

        if not obj:
            continue

        url = client.generate_presigned_url(
            "get_object",
            ExpiresIn=3600,
            Params={"Bucket": "cdn.future-fdn.tech", "Key": obj.key},
        ).replace("s3.amazonaws.com/", "")
        # versions = client.list_object_versions(
        #     Bucket="cdn.future-fdn.tech", Prefix=file.type.title() + "/" + file.file_name
        # )

        file = file.to_dict()
        user = await session.scalar(select(User).where(User.id == file["user_id"]))

        file["name"] = user.name
        # file["versions"] = [
        #     {
        #         "id": x["VersionId"],
        #         "latest": x["IsLatest"],
        #         "modified": x["LastModified"],
        #         "key": x["Key"],
        #     }
        #     for x in versions["Versions"]
        # ]

        files.append(
            {
                **file,
                "url": url,
            }
        )

    return {"files": files, "total": len(files)}


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

    client = aws_session.client("s3")

    def read_file(file: File) -> pd.DataFrame:
        url = client.generate_presigned_url(
            "get_object",
            ExpiresIn=3600,
            Params={
                "Bucket": "cdn.future-fdn.tech",
                "Key": file.type.title() + "/" + file.file_name,
            },
        ).replace("s3.amazonaws.com/", "")

        if file.file_name.endswith(".csv"):
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
        map_data, df, master_df, query_column, master_column, file
    )

    return {"detail": "Added to tasks successfully"}


async def map_data(df: pd.DataFrame, master_df, query_column, master_column, file):
    session = get_async_session()

    for col in master_df.columns:
        if col.startswith("Unnamed"):
            del master_df[col]

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
        "cdn.future-fdn.tech",
        "result" + "/" + file.file_name,
    )

    await session.scalar(
        update(Task)
        .where(Task.file_id == file.id)
        .values(ended=datetime.now(), status="COMPLETED")
        .returning(Task)
    )

    await session.commit()

    del df
    del master_df


# @router.post(
#     "/files",
#     status_code=status.HTTP_201_CREATED,
# )
# async def create_file_object(
#     file: UploadFile, session: AsyncSession = Depends(get_session)
# ):
# ...
# try:
#     contents = await file.read()
#     async with aiofiles.open(file.filename, 'wb') as f:
#         await f.write(contents)
# except Exception:
#     raise HTTPException(
#         status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#         detail='There was an error uploading the file',
#     )
# finally:
#     await file.close()

# file_name = file.filename

# file = await session.scalar(insert(File) )
