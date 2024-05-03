"""add total column in file

Revision ID: 2f7250dcee53
Revises: e212b16e066d
Create Date: 2024-05-03 05:48:28.052370

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2f7250dcee53"
down_revision: Union[str, None] = "e212b16e066d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("files", sa.Column("total", sa.Integer()))


def downgrade() -> None:
    pass
