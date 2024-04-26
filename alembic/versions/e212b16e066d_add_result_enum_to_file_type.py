"""add result enum to file type

Revision ID: e212b16e066d
Revises: b34f615f9568
Create Date: 2024-04-26 05:33:37.301449

"""

from typing import Sequence, Union

from alembic import op

revision: str = "e212b16e066d"
down_revision: Union[str, None] = "b34f615f9568"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE file_type ADD VALUE 'RESULT'")


def downgrade() -> None:
    pass
