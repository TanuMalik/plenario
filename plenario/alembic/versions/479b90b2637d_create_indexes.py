"""Create indexes

Revision ID: 479b90b2637d
Revises: 42eee6f4f98c
Create Date: 2015-11-30 08:58:15.522826

"""

# revision identifiers, used by Alembic.
revision = '479b90b2637d'
down_revision = '42eee6f4f98c'
branch_labels = None
depends_on = None

import os
import sys

pwd = os.path.dirname(os.path.realpath(__file__))
plenario_path = os.path.join(pwd, '../../..')
sys.path.append(str(plenario_path))

from plenario.alembic.version_helpers import dataset_names
from alembic import op


def truncated_names():
    for name in dataset_names():
        # Indexes can only be 63 chars long.
        # So make the part of the index name determined by the dataset be max 45 chars
        yield name[:45]


def upgrade():
    for name in dataset_names():
        # Indexes can only be 63 chars long.
        # So make the part of the index name determined by the dataset be max 45 chars
        trunc = name[:45]
        table_name = 'dat_{}'.format(name)

        op.create_index('ix_{}_point_id'.format(trunc), table_name, ['point_id'])
        op.create_index('ix_{}_point_date'.format(trunc), table_name, ['point_date'])
        op.create_index('ix_{}_point_geom'.format(trunc), table_name, ['geom'])


def downgrade():
    for name in truncated_names():
        trunc = name[:45]
        op.drop_index('ix_{}_point_id'.format(trunc))
        op.drop_index('ix_{}_point_date'.format(trunc))
        op.drop_index('ix_{}_point_geom'.format(trunc))
