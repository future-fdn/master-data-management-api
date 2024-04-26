import uuid
from datetime import datetime
from typing import List

from pydantic import BaseModel
from sqlalchemy import Date, DateTime, Enum, Float, Integer, String, Uuid, func
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
        Enum("MASTER", "QUERY", "RESULT", name="file_type"), nullable=False
    )

    def to_dict(self):
        return {field.name: getattr(self, field.name) for field in self.__table__.c}


class DQBase(DeclarativeBase): ...


class DataQuality(DQBase):
    __tablename__ = "data_quality"

    date: Mapped[Date] = mapped_column(
        Date(),
        primary_key=True,
    )
    overall_uniqueness: Mapped[float] = mapped_column(Float())
    overall_completeness: Mapped[float] = mapped_column(Float())
    total_query_records: Mapped[int] = mapped_column(Integer())
    total_master_records: Mapped[int] = mapped_column(Integer())

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


class FileStats(BaseModel):
    overall_completeness: str
    completeness_diff: str
    overall_uniqueness: str
    uniqueness_diff: str
    total_query_records: int
    query_records_diff: int
    total_master_records: int
    master_records_diff: int


class FilesResponse(BaseModel):
    files: List[FileResponse]
    total: int


class Graph(BaseModel):
    date: str
    value: int


class GraphResponse(BaseModel):
    datas: List[Graph]
