"""add dashboard

Revision ID: b34f615f9568
Revises: 54beace3aee1
Create Date: 2024-04-26 01:27:26.999813

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b34f615f9568"
down_revision: Union[str, None] = "54beace3aee1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "data_quality",
        sa.Column("date", sa.Date(), server_default=sa.text("now()"), nullable=False),
        sa.Column("overall_completeness", sa.Float()),
        sa.Column("overall_uniqueness", sa.Float()),
        sa.Column("total_query_records", sa.Integer()),
        sa.Column("total_master_records", sa.Integer()),
        sa.PrimaryKeyConstraint("date"),
    )


def downgrade() -> None:
    pass
