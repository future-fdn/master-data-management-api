"""add description to file table

Revision ID: 13ea6c8aa3ab
Revises: c6d49c9f1660
Create Date: 2024-03-11 03:50:49.237429

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "13ea6c8aa3ab"
down_revision: Union[str, None] = "c6d49c9f1660"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("files", sa.Column("description", sa.String(100)))


def downgrade() -> None:
    pass
