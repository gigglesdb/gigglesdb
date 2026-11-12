"""Initial shop schema, kept immutable alongside the sample models."""
from alembic import op
import sqlalchemy as sa

revision = '0001'
down_revision = None


def upgrade():
    op.create_table('shop_user', sa.Column('id', sa.Integer, primary_key=True, autoincrement=False),
                    sa.Column('email', sa.String(120), nullable=False), sa.UniqueConstraint('email'))
    op.create_table('shop_order', sa.Column('id', sa.Integer, primary_key=True, autoincrement=False),
                    sa.Column('user_id', sa.Integer, sa.ForeignKey('shop_user.id'), nullable=False),
                    sa.Column('status', sa.String(20), nullable=False),
                    sa.Column('total', sa.Numeric(12, 2), nullable=False),
                    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False))
    op.create_table('shop_orderitem', sa.Column('order_id', sa.Integer, sa.ForeignKey('shop_order.id'), primary_key=True),
                    sa.Column('sku', sa.String(40), primary_key=True), sa.Column('quantity', sa.Integer, nullable=False))
    for table, column in [('shop_order', 'user_id'), ('shop_order', 'status'), ('shop_orderitem', 'order_id')]:
        op.create_index('ix_' + table + '_' + column, table, [column])


def downgrade():
    for table in ['shop_orderitem', 'shop_order', 'shop_user']:
        op.drop_table(table)
