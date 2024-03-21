"""add tables

Revision ID: 54beace3aee1
Revises:
Create Date: 2024-03-22 00:07:48.834955

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "54beace3aee1"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Uuid(as_uuid=False), nullable=False),
        sa.Column("name", sa.String(100)),
        sa.Column("email", sa.String(400)),
        sa.Column("password", sa.String(100)),
        sa.Column(
            "created",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("role", sa.Enum("ADMIN", "USER", name="role"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "files",
        sa.Column("id", sa.Uuid(as_uuid=False), nullable=False),
        sa.Column("file_name", sa.String(200), nullable=False),
        sa.Column("description", sa.String(100)),
        sa.Column("user_id", sa.Uuid(as_uuid=False)),
        sa.ForeignKeyConstraint(
            ("user_id",),
            ["users.id"],
        ),
        sa.Column(
            "created",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "modified",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("unique", sa.Integer()),
        sa.Column("valid", sa.Integer()),
        sa.Column("type", sa.Enum("MASTER", "QUERY", name="file_type"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "tasks",
        sa.Column("id", sa.Uuid(as_uuid=False), nullable=False),
        sa.Column("file_id", sa.Uuid(as_uuid=False), nullable=False),
        sa.ForeignKeyConstraint(
            ("file_id",),
            ["files.id"],
        ),
        sa.Column("user_id", sa.Uuid(as_uuid=False)),
        sa.ForeignKeyConstraint(
            ("user_id",),
            ["users.id"],
        ),
        sa.Column(
            "started",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "ended",
            sa.DateTime(timezone=True),
        ),
        sa.Column(
            "status",
            sa.Enum(
                "PENDING", "IN_PROGRESS", "COMPLETED", "FAILED", name="task_status"
            ),
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    pass
