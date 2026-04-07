"""add dry-run columns to copied_trades

Adds is_simulated flag to support paper trading mode.
(settled_at was already added in the initial migration.)

Revision ID: a1b2c3d4e5f6
Revises: c3f7a2e5b891
Create Date: 2026-04-03 00:02:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'c3f7a2e5b891'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add is_simulated column to copied_trades for dry-run / paper trading mode."""
    op.add_column(
        'copied_trades',
        sa.Column('is_simulated', sa.Boolean(), nullable=False, server_default='0'),
    )


def downgrade() -> None:
    """Remove is_simulated column from copied_trades."""
    op.drop_column('copied_trades', 'is_simulated')
