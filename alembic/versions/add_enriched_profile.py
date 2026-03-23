"""Add enriched profile fields to users table.

Revision ID: enriched_profile_001
"""
from alembic import op
import sqlalchemy as sa

revision = 'enriched_profile_001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # Add enriched profile columns (ignore if already exist)
    columns = [
        ('cultures', sa.String(), True),
        ('superficie', sa.Float(), True),
        ('genre', sa.String(), True),
        ('age', sa.Integer(), True),
        ('experience', sa.Integer(), True),
        ('type_exploitation', sa.String(), True),
        ('membre_cooperative', sa.String(), True),
        ('profil_type', sa.String(), True),
    ]
    for col_name, col_type, nullable in columns:
        try:
            op.add_column('users', sa.Column(col_name, col_type, nullable=nullable))
        except Exception:
            pass  # Column already exists


def downgrade():
    columns = ['cultures', 'superficie', 'genre', 'age', 'experience',
               'type_exploitation', 'membre_cooperative', 'profil_type']
    for col_name in columns:
        try:
            op.drop_column('users', col_name)
        except Exception:
            pass
