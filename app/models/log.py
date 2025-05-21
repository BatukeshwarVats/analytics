import datetime
import json
from sqlalchemy import Column, Integer, String, DateTime, Text, Index, func
from sqlalchemy.dialects.postgresql import JSONB
from app.database.base import Base
from app.core.enums import ProcessingStatus


class SparkEventLog(Base):
    """SQLAlchemy model for storing raw Spark event logs."""
    __tablename__ = "spark_event_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    event_type = Column(String(100), nullable=False, index=True)
    job_id = Column(Integer, nullable=False, index=True)
    user = Column(String(255), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    payload = Column(JSONB, nullable=False)
    ingestion_time = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    processing_status = Column(
        String(20), 
        default=ProcessingStatus.PENDING.value, 
        nullable=False,
        index=True
    )
    processing_time = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Create a composite index to help with log grouping and deduplication
    __table_args__ = (
        Index('idx_job_event_type', 'job_id', 'event_type'),
        Index('idx_timestamp_job_id', 'timestamp', 'job_id'),
        Index('idx_unprocessed_logs', 'processing_status', 'job_id', 'event_type'),
    )
    
    def __init__(
        self, 
        event_type: str, 
        job_id: int, 
        user: str, 
        timestamp: datetime.datetime, 
        payload: dict
    ):
        self.event_type = event_type
        self.job_id = job_id
        self.user = user
        self.timestamp = timestamp
        self.payload = payload
        
    def __repr__(self) -> str:
        return f"<SparkEventLog(id={self.id}, job_id={self.job_id}, event_type={self.event_type})>"
        
    @classmethod
    def from_json(cls, data: dict) -> "SparkEventLog":
        """
        Create a SparkEventLog instance from a JSON payload.
        
        Args:
            data: The JSON payload from the API
            
        Returns:
            SparkEventLog instance
        """
        # Parse ISO 8601 timestamp string to datetime
        timestamp = datetime.datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
        
        return cls(
            event_type=data["event"],
            job_id=data["job_id"],
            user=data["user"],
            timestamp=timestamp,
            payload=data
        ) 