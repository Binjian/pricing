"""Empty Init

Revision ID: f4a0835d16c9
Revises: 
Create Date: 2024-09-21 20:26:43.596966

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f4a0835d16c9'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.rename_table('price_training_raw_2024_usd', 'price_training_zone_label_2024_usd')
    pass


def downgrade() -> None:
    op.rename_table('price_training_zone_label_2024_usd', 'price_training_raw_2024_usd')
    pass
