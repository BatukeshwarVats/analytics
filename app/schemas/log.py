from datetime import datetime
from typing import Dict, Any, Literal, Optional
from pydantic import BaseModel, Field, EmailStr, root_validator, RootModel
from app.core.enums import SparkEventType


class SparkEventBase(BaseModel):
    """Base schema for all Spark events with common fields."""
    event: str = Field(..., description="Type of Spark event")
    job_id: int = Field(..., description="Unique identifier for the job")
    user: EmailStr = Field(..., description="User email who submitted the job")
    timestamp: datetime = Field(..., description="ISO 8601 formatted timestamp")
    
    @root_validator(pre=True)
    def check_event_type(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Validate that the event type is one of the supported types."""
        event = values.get("event")
        if event and event not in [e.value for e in SparkEventType]:
            raise ValueError(f"Event type '{event}' is not supported. Must be one of: {', '.join([e.value for e in SparkEventType])}")
        return values


class SparkListenerJobStart(SparkEventBase):
    """Schema for SparkListenerJobStart events."""
    event: Literal[SparkEventType.JOB_START.value] = Field(..., description="Job start event type")


class SparkListenerTaskEnd(SparkEventBase):
    """Schema for SparkListenerTaskEnd events."""
    event: Literal[SparkEventType.TASK_END.value] = Field(..., description="Task end event type")
    task_id: str = Field(..., description="Unique identifier for the task")
    duration_ms: int = Field(..., description="Duration of the task in milliseconds")
    successful: bool = Field(..., description="Whether the task was successful")


class SparkListenerJobEnd(SparkEventBase):
    """Schema for SparkListenerJobEnd events."""
    event: Literal[SparkEventType.JOB_END.value] = Field(..., description="Job end event type")
    completion_time: datetime = Field(..., description="ISO 8601 formatted completion timestamp")
    job_result: str = Field(..., description="Result of the job (JobSucceeded or JobFailed)")


class SparkEventCreate(RootModel):
    """Union schema for all event types during creation."""
    root: Dict[str, Any] = Field(..., description="Raw event data")


class LogIngestResponse(BaseModel):
    """Response schema for log ingestion."""
    success: bool = Field(..., description="Whether ingestion was successful")
    message: str = Field(..., description="Status message")
    log_id: Optional[int] = Field(None, description="ID of the ingested log") 