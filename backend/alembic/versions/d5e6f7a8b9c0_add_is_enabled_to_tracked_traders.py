"""add is_enabled to tracked_traders

Adds is_enabled flag to allow excluding a specific trader from signal
generation without touching is_active (which reflects Kalshi leaderboard
presence).

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
Create Date: 2026-04-03 00:05:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd5e6f7a8b9c0'
down_revision: Union[str, Sequence[str], None] = 'c4d5e6f7a8b9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add is_enabled column to tracked_traders."""
    op.add_column(
        'tracked_traders',
        sa.Column('is_enabled', sa.Boolean(), nullable=False, server_default='1'),
    )


def downgrade() -> None:
    """Remove is_enabled column from tracked_traders."""
    op.drop_column('tracked_traders', 'is_enabled')
