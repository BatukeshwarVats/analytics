from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import Any, Dict

from app.database.base import get_db
from app.schemas.analytics import JobAnalyticsResponse, DailySummaryResponse
from app.services.analytics_service import AnalyticsService

router = APIRouter()


@router.get("/jobs/{job_id}", response_model=JobAnalyticsResponse)
def get_job_analytics(
    job_id: int,
    db: Session = Depends(get_db)
) -> JobAnalyticsResponse:
    """
    Get analytics for a specific job.
    
    This endpoint returns the calculated analytics for a job, including:
    - Duration
    - Task count
    - Task failure rate
    - Overall job status
    
    If the job analytics are not available (e.g., job is still in progress or
    has not been processed yet), a 404 error is returned.
    
    Args:
        job_id: ID of the job to get analytics for
        
    Returns:
        JobAnalyticsResponse with all job metrics
    """
    analytics = AnalyticsService.get_job_analytics(db, job_id)
    
    if not analytics:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Analytics for job ID {job_id} not found"
        )
    
    return JobAnalyticsResponse.from_orm(analytics)


@router.get("/summary", response_model=DailySummaryResponse)
def get_daily_summary(
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    db: Session = Depends(get_db)
) -> DailySummaryResponse:
    """
    Get summary analytics for all jobs completed on a specific date.
    
    This endpoint returns aggregate statistics for all jobs completed on the
    specified date, including:
    - Total number of jobs
    - Average job duration
    - Average task success rate
    - List of job summaries
    
    Args:
        date: Date string in YYYY-MM-DD format
        
    Returns:
        DailySummaryResponse with aggregate statistics
    """
    try:
        summary = AnalyticsService.get_daily_summary(db, date)
        return DailySummaryResponse(**summary)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        ) 