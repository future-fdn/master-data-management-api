from datetime import datetime
from io import BytesIO
from typing import Annotated, Literal, Union
from uuid import UUID

import boto3
import pandas as pd
from anyio import Path
from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, status
from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from thefuzz import fuzz

from app.config import settings
from app.db import engine
from app.db.base import get_session
from app.files.models import File, FileResponse, FilesResponse
from app.tasks.models import Task
from app.users.models import User
from app.utils.auth import get_current_user

router = APIRouter()

aws_session = boto3.Session(
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_ACCESS_KEY,
)
s3 = aws_session.resource("s3")
client = aws_session.client("s3")
bucket = s3.Bucket("mdmfiles")


@router.get("/files/{file_id}", response_model=Union[FilesResponse | FileResponse])
async def get_specific_files(
    file_id: UUID | Literal["master", "query"],
    session: AsyncSession = Depends(get_session),
):
    files = []
    url = None

    if file_id == "master":
        obj_list = bucket.objects.filter(Prefix="Master/")
    elif file_id == "query":
        obj_list = bucket.objects.filter(Prefix="Query/")
    else:
        file = await session.scalar(select(File).where(File.id == file_id))
        obj = [
            x
            for x in bucket.objects.filter(
                Prefix=file.type.title() + "/" + file.file_name
            )
        ][0]

        if not url:
            url = client.generate_presigned_url(
                "get_object",
                ExpiresIn=3600,
                Params={"Bucket": "mdmfiles", "Key": obj.key},
            )
        # versions = client.list_object_versions(
        #     Bucket="mdmfiles", Prefix=file.type.title() + "/" + file.file_name
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

    for obj in obj_list:
        if obj.get()["ContentType"].startswith("application/x-directory"):
            continue

        file = await session.scalar(
            select(File).where(File.file_name == Path(obj.key).name)
        )

        if not file:
            continue

        if not url:
            url = client.generate_presigned_url(
                "get_object",
                ExpiresIn=3600,
                Params={"Bucket": "mdmfiles", "Key": obj.key},
            )
        # versions = client.list_object_versions(
        #     Bucket="mdmfiles", Prefix=file.type.title() + "/" + file.file_name
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
async def get_all_file(session: AsyncSession = Depends(get_session)):
    files = []
    url = None

    obj_list = bucket.objects.all()

    for obj in obj_list:
        if obj.get()["ContentType"].startswith("application/x-directory"):
            continue

        file = await session.scalar(
            select(File).where(File.file_name == Path(obj.key).name)
        )

        if not file:
            continue

        if not url:
            url = client.generate_presigned_url(
                "get_object",
                ExpiresIn=3600,
                Params={"Bucket": "mdmfiles", "Key": obj.key},
            )
        # versions = client.list_object_versions(
        #     Bucket="mdmfiles", Prefix=file.type.title() + "/" + file.file_name
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
                "Bucket": "mdmfiles",
                "Key": file.type.title() + "/" + file.file_name,
            },
        )

        if file.file_name.endswith(".csv"):
            df = pd.read_csv(url)
        elif file.file_name.endswith(".txt"):
            df = pd.read_fwf(url, header=None)
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
        .values(file_id=file.id, user_id=current_user.id, type="PENDING")
        .returning(Task)
    )

    if not task:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="There was an error creating the task",
        )

    background_tasks.add_task(
        map_data, df, master_df, query_column, master_column, file
    )

    return {"detail": "Added to tasks successfully"}


async def map_data(df: pd.DataFrame, master_df, query_column, master_column, file):
    for col in master_df.columns:
        if col.startswith("Unnamed"):
            del master_df[col]

    if not any(master_column in master_df.columns):
        ...
    if not any(query_column in df.columns):
        ...  # TODO: Error

    for i, row in enumerate(df[query_column].rows):
        partial = master_df[master_column].apply(lambda x: (fuzz.partial_ratio(x, row)))
        full = master_df[master_column].apply(lambda x: (fuzz.ratio(x, row)))

        if full.max() > 90:
            df[query_column][i] = master_df[master_column][partial.idxmax()]
        elif partial.max() > 90:
            df[query_column][i] = master_df[master_column][partial.idxmax()]
        else:
            df[query_column][i] = master_df[master_column][partial.idxmax()]

    buffer = BytesIO()
    df.to_csv(buffer, index=False)

    while buffer.readable():
        client.upload_fileobj(
            buffer.read(65535), "mdmfiles", file.type.title() + "/" + file.file_name
        )

    async with engine.get_async_session() as session:
        await session.scalar(
            update(Task)
            .where(file_id=file.id, modified=datetime.now())
            .values("COMPLETED")
        )
        await session.scalar(
            insert(File).values(
                unique=df[query_column].unique().shape[1],
                valid=int(df[query_column].count()),
            )
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
