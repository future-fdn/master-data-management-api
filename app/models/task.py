import uuid
from datetime import datetime
from typing import List

from pydantic import BaseModel
from sqlalchemy import DateTime, Enum, Uuid, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    started: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    ended: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), onupdate=func.now()
    )


class Type(Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False), primary_key=True, default=lambda _: str(uuid.uuid4())
    )
    file_id: Mapped[str] = mapped_column(Uuid(as_uuid=False))
    user_id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False), nullable=False, unique=True
    )
    status: Mapped[Type] = mapped_column(
        Enum("PENDING", "IN_PROGRESS", "COMPLETED", "FAILED", name="task_status"),
        nullable=False,
    )

    def to_dict(self):
        return {field.name: getattr(self, field.name) for field in self.__table__.c}


class TaskResponse(BaseModel):
    id: uuid.UUID
    file_id: uuid.UUID
    file_name: str
    user_id: uuid.UUID
    user_name: str
    status: str
    started: datetime
    ended: datetime
    url: str


class TasksResponse(BaseModel):
    tasks: List[TaskResponse]
    total: int
