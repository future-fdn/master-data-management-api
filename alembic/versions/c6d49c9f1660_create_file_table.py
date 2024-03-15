"""create file table

Revision ID: c6d49c9f1660
Revises: 300d3c404c3d
Create Date: 2024-03-11 03:24:01.987737

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c6d49c9f1660"
down_revision: Union[str, None] = "300d3c404c3d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "files",
        sa.Column("id", sa.Uuid(as_uuid=False), nullable=False),
        sa.Column("file_name", sa.String(200), nullable=False),
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


def downgrade() -> None:
    pass
