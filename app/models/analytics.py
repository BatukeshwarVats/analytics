import datetime
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, Index, UniqueConstraint
from app.database.base import Base


class JobAnalytics(Base):
    """SQLAlchemy model for storing job analytics results."""
    __tablename__ = "job_analytics"
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, nullable=False, unique=True, index=True)
    user = Column(String(255), nullable=False, index=True)
    
    # Job timing information
    start_time = Column(DateTime, nullable=False, index=True)
    end_time = Column(DateTime, nullable=False)
    duration_seconds = Column(Float, nullable=False)
    
    # Task statistics
    task_count = Column(Integer, nullable=False, default=0)
    failed_tasks = Column(Integer, nullable=False, default=0)
    success_rate = Column(Float, nullable=False, default=0.0)
    
    # Job status
    job_result = Column(String(50), nullable=False)
    
    # Processing metadata
    created_at = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)
    
    # Create a composite index for better query performance
    __table_args__ = (
        Index('idx_analytics_date_user', 'start_time', 'user'),
    )
    
    def __init__(
        self,
        job_id: int,
        user: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        duration_seconds: float,
        task_count: int,
        failed_tasks: int,
        success_rate: float,
        job_result: str
    ):
        self.job_id = job_id
        self.user = user
        self.start_time = start_time
        self.end_time = end_time
        self.duration_seconds = duration_seconds
        self.task_count = task_count
        self.failed_tasks = failed_tasks
        self.success_rate = success_rate
        self.job_result = job_result
        
    def __repr__(self) -> str:
        return f"<JobAnalytics(job_id={self.job_id}, duration={self.duration_seconds}s, success_rate={self.success_rate}%)>"
        
    def to_dict(self) -> dict:
        """Convert the model to a dictionary for API responses."""
        return {
            "job_id": self.job_id,
            "user": self.user,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration_seconds": round(self.duration_seconds, 2),
            "task_count": self.task_count,
            "failed_tasks": self.failed_tasks,
            "success_rate": round(self.success_rate, 2),
            "job_result": self.job_result
        } 