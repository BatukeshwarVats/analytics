"""Initial database migration

Revision ID: initial
Create Date: 2023-07-01

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'initial'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create spark_event_logs table
    op.create_table(
        'spark_event_logs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('event_type', sa.String(length=100), nullable=False),
        sa.Column('job_id', sa.Integer(), nullable=False),
        sa.Column('user', sa.String(length=255), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('ingestion_time', sa.DateTime(), nullable=False),
        sa.Column('processing_status', sa.String(length=20), nullable=False),
        sa.Column('processing_time', sa.DateTime(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes on spark_event_logs
    op.create_index(op.f('ix_spark_event_logs_event_type'), 'spark_event_logs', ['event_type'], unique=False)
    op.create_index(op.f('ix_spark_event_logs_id'), 'spark_event_logs', ['id'], unique=False)
    op.create_index(op.f('ix_spark_event_logs_job_id'), 'spark_event_logs', ['job_id'], unique=False)
    op.create_index(op.f('ix_spark_event_logs_processing_status'), 'spark_event_logs', ['processing_status'], unique=False)
    op.create_index(op.f('ix_spark_event_logs_timestamp'), 'spark_event_logs', ['timestamp'], unique=False)
    op.create_index(op.f('ix_spark_event_logs_user'), 'spark_event_logs', ['user'], unique=False)
    op.create_index('idx_job_event_type', 'spark_event_logs', ['job_id', 'event_type'], unique=False)
    op.create_index('idx_timestamp_job_id', 'spark_event_logs', ['timestamp', 'job_id'], unique=False)
    op.create_index('idx_unprocessed_logs', 'spark_event_logs', ['processing_status', 'job_id', 'event_type'], unique=False)
    
    # Create job_analytics table
    op.create_table(
        'job_analytics',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_id', sa.Integer(), nullable=False),
        sa.Column('user', sa.String(length=255), nullable=False),
        sa.Column('start_time', sa.DateTime(), nullable=False),
        sa.Column('end_time', sa.DateTime(), nullable=False),
        sa.Column('duration_seconds', sa.Float(), nullable=False),
        sa.Column('task_count', sa.Integer(), nullable=False),
        sa.Column('failed_tasks', sa.Integer(), nullable=False),
        sa.Column('success_rate', sa.Float(), nullable=False),
        sa.Column('job_result', sa.String(length=50), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes on job_analytics
    op.create_index(op.f('ix_job_analytics_id'), 'job_analytics', ['id'], unique=False)
    op.create_index(op.f('ix_job_analytics_job_id'), 'job_analytics', ['job_id'], unique=True)
    op.create_index(op.f('ix_job_analytics_start_time'), 'job_analytics', ['start_time'], unique=False)
    op.create_index(op.f('ix_job_analytics_user'), 'job_analytics', ['user'], unique=False)
    op.create_index('idx_analytics_date_user', 'job_analytics', ['start_time', 'user'], unique=False)


def downgrade() -> None:
    # Drop job_analytics table and its indexes
    op.drop_index('idx_analytics_date_user', table_name='job_analytics')
    op.drop_index(op.f('ix_job_analytics_user'), table_name='job_analytics')
    op.drop_index(op.f('ix_job_analytics_start_time'), table_name='job_analytics')
    op.drop_index(op.f('ix_job_analytics_job_id'), table_name='job_analytics')
    op.drop_index(op.f('ix_job_analytics_id'), table_name='job_analytics')
    op.drop_table('job_analytics')
    
    # Drop spark_event_logs table and its indexes
    op.drop_index('idx_unprocessed_logs', table_name='spark_event_logs')
    op.drop_index('idx_timestamp_job_id', table_name='spark_event_logs')
    op.drop_index('idx_job_event_type', table_name='spark_event_logs')
    op.drop_index(op.f('ix_spark_event_logs_user'), table_name='spark_event_logs')
    op.drop_index(op.f('ix_spark_event_logs_timestamp'), table_name='spark_event_logs')
    op.drop_index(op.f('ix_spark_event_logs_processing_status'), table_name='spark_event_logs')
    op.drop_index(op.f('ix_spark_event_logs_job_id'), table_name='spark_event_logs')
    op.drop_index(op.f('ix_spark_event_logs_id'), table_name='spark_event_logs')
    op.drop_index(op.f('ix_spark_event_logs_event_type'), table_name='spark_event_logs')
    op.drop_table('spark_event_logs') 