"""renaming price_training_raw_2024_usd to price_training_zone_label_2024_usd

Revision ID: 5b321f641caa
Revises: f4a0835d16c9
Create Date: 2024-09-21 20:42:09.315670

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5b321f641caa'
down_revision: Union[str, None] = 'f4a0835d16c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
