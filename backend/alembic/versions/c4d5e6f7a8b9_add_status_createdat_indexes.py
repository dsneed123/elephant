"""add status and created_at indexes for TradeSignal and CopiedTrade

APScheduler tasks query these tables by status and created_at every 2-15 minutes;
without indexes those are full table scans. Adds individual and composite indexes.
Note: ix_copied_trades_status already exists from revision c3f7a2e5b891.

Revision ID: c4d5e6f7a8b9
Revises: b2c3d4e5f6a7
Create Date: 2026-04-03 00:04:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c4d5e6f7a8b9'
down_revision: Union[str, Sequence[str], None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add status and created_at indexes for scheduler query performance."""
    # trade_signals: individual indexes
    op.create_index(
        op.f('ix_trade_signals_status'),
        'trade_signals',
        ['status'],
        unique=False,
    )
    op.create_index(
        op.f('ix_trade_signals_created_at'),
        'trade_signals',
        ['created_at'],
        unique=False,
    )
    # trade_signals: composite index for status + created_at queries
    op.create_index(
        'ix_trade_signals_status_created_at',
        'trade_signals',
        ['status', 'created_at'],
        unique=False,
    )

    # copied_trades: ix_copied_trades_status already exists (revision c3f7a2e5b891)
    op.create_index(
        op.f('ix_copied_trades_created_at'),
        'copied_trades',
        ['created_at'],
        unique=False,
    )
    # copied_trades: composite index for status + created_at queries
    op.create_index(
        'ix_copied_trades_status_created_at',
        'copied_trades',
        ['status', 'created_at'],
        unique=False,
    )


def downgrade() -> None:
    """Remove status and created_at indexes."""
    op.drop_index('ix_copied_trades_status_created_at', table_name='copied_trades')
    op.drop_index(op.f('ix_copied_trades_created_at'), table_name='copied_trades')
    op.drop_index('ix_trade_signals_status_created_at', table_name='trade_signals')
    op.drop_index(op.f('ix_trade_signals_created_at'), table_name='trade_signals')
    op.drop_index(op.f('ix_trade_signals_status'), table_name='trade_signals')
