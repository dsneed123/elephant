"""add rejection_reason to copied_trades

Adds rejection_reason column to record why a trade was cancelled due to
failed liquidity checks (insufficient depth or wide bid-ask spread).

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
    """Add rejection_reason column to copied_trades."""
    op.add_column(
        'copied_trades',
        sa.Column('rejection_reason', sa.String(), nullable=True),
    )


def downgrade() -> None:
    """Remove rejection_reason column from copied_trades."""
    op.drop_column('copied_trades', 'rejection_reason')
