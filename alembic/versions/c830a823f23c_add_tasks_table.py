"""add tasks table

Revision ID: c830a823f23c
Revises: 695cc34c02c2
Create Date: 2024-03-14 22:51:55.877990

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c830a823f23c"
down_revision: Union[str, None] = "695cc34c02c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
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
            sa.Enum("PENDING", "IN_PROGRESS", "COMPLETED", name="task_status"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    pass
