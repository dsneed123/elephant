"""add copied_trades status index

Revision ID: c3f7a2e5b891
Revises: 450ea6e8b6ad
Create Date: 2026-04-03 00:01:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3f7a2e5b891'
down_revision: Union[str, Sequence[str], None] = '450ea6e8b6ad'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add index on copied_trades.status for settlement query performance."""
    op.create_index(
        op.f('ix_copied_trades_status'),
        'copied_trades',
        ['status'],
        unique=False,
    )


def downgrade() -> None:
    """Remove copied_trades.status index."""
    op.drop_index(op.f('ix_copied_trades_status'), table_name='copied_trades')
