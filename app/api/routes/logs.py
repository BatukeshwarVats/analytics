from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import Any, Dict

from app.database.base import get_db
from app.schemas.log import SparkEventCreate, LogIngestResponse
from app.services.log_service import LogService
from app.workers.tasks import process_specific_job

router = APIRouter()


@router.post("/ingest", response_model=LogIngestResponse)
def ingest_log(
    log_data: Dict[str, Any],
    db: Session = Depends(get_db)
) -> LogIngestResponse:
    """
    Ingest a new Spark event log.
    
    This endpoint accepts a JSON payload containing a valid Spark event log.
    The log is stored in the database and scheduled for async processing.
    
    Example:
    ```
    {
        "event": "SparkListenerJobStart",
        "job_id": 101,
        "timestamp": "2024-03-30T10:12:45Z",
        "user": "data_engineer_1@example.com"
    }
    ```
    
    Returns a response with success status, message, and log ID.
    """
    # Ingest the log
    success, message, log_id = LogService.ingest_log(db, log_data)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=message
        )
    
    # If this is a JobEnd event, trigger processing for this job
    # This optimizes the case where we receive events in order
    if log_data.get("event") == "SparkListenerJobEnd":
        job_id = log_data.get("job_id")
        # Use eager=False to queue the task for async processing
        process_specific_job.delay(job_id)
    
    return LogIngestResponse(
        success=success,
        message=message,
        log_id=log_id
    ) 