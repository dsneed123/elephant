"""add has_trade_history to tracked_traders

Adds has_trade_history flag to distinguish seeded win_rate priors from
real trade-history-derived values.

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-04-03 00:03:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add has_trade_history column to tracked_traders."""
    op.add_column(
        'tracked_traders',
        sa.Column('has_trade_history', sa.Boolean(), nullable=False, server_default='0'),
    )


def downgrade() -> None:
    """Remove has_trade_history column from tracked_traders."""
    op.drop_column('tracked_traders', 'has_trade_history')
