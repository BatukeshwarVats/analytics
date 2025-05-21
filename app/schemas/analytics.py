from datetime import datetime, date as date_type
from typing import List, Optional
from pydantic import BaseModel, Field


class JobAnalyticsResponse(BaseModel):
    """Response schema for job analytics."""
    job_id: int = Field(..., description="Unique identifier for the job")
    user: str = Field(..., description="User email who submitted the job")
    start_time: datetime = Field(..., description="Start time of the job")
    end_time: datetime = Field(..., description="End time of the job")
    duration_seconds: float = Field(..., description="Duration of the job in seconds")
    task_count: int = Field(..., description="Total number of tasks in the job")
    failed_tasks: int = Field(..., description="Number of failed tasks")
    success_rate: float = Field(..., description="Task success rate as a percentage")
    job_result: str = Field(..., description="Final result of the job")
    
    class Config:
        """Pydantic config for the schema."""
        from_attributes = True


class JobSummary(BaseModel):
    """Summary of a single job for the daily summary."""
    job_id: int
    user: str
    duration_seconds: float
    task_count: int
    success_rate: float
    job_result: str


class DailySummaryResponse(BaseModel):
    """Response schema for daily summary analytics."""
    date: date_type = Field(..., description="Date for the summary")
    total_jobs: int = Field(..., description="Total number of jobs completed on this date")
    avg_duration_seconds: float = Field(..., description="Average job duration in seconds")
    avg_success_rate: float = Field(..., description="Average task success rate across all jobs")
    jobs: List[JobSummary] = Field(..., description="List of jobs completed on this date") 