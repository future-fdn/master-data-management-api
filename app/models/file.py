import uuid
from datetime import datetime
from typing import List

from pydantic import BaseModel
from sqlalchemy import DateTime, Enum, Integer, String, Uuid, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    created: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    modified: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class Type(Enum):
    MASTER = "MASTER"
    QUERY = "QUERY"


class File(Base):
    __tablename__ = "files"

    id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False), primary_key=True, default=lambda _: str(uuid.uuid4())
    )
    file_name: Mapped[str] = mapped_column(String(200), nullable=False)
    user_id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False), nullable=False, unique=True
    )
    description: Mapped[str] = mapped_column(String(100), nullable=False)
    unique: Mapped[int] = mapped_column(Integer(), nullable=False)
    valid: Mapped[int] = mapped_column(Integer(), nullable=False)
    type: Mapped[Type] = mapped_column(
        Enum("MASTER", "QUERY", name="file_type"), nullable=False
    )

    def to_dict(self):
        return {field.name: getattr(self, field.name) for field in self.__table__.c}


class Version(BaseModel):
    id: str
    latest: bool
    modified: datetime


class FileResponse(BaseModel):
    id: uuid.UUID
    description: str
    name: str
    file_name: str
    user_id: str
    unique: int
    valid: int
    type: str
    created: datetime
    modified: datetime
    url: str
    # versions: List[Version]


class FilesResponse(BaseModel):
    files: List[FileResponse]
    total: int
