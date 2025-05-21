import json
import datetime
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_, cast, Date
from app.models.log import SparkEventLog
from app.models.analytics import JobAnalytics
from app.core.redis import redis_client
from app.core.enums import ProcessingStatus, SparkEventType, JobResult


class AnalyticsService:
    """Service for processing and retrieving analytics data."""
    
    @staticmethod
    def process_job(db: Session, job_id: int) -> bool:
        """
        Process analytics for a job.
        
        Args:
            db: Database session
            job_id: Job ID to process
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            # Check if analytics already exist for this job
            existing_analytics = db.query(JobAnalytics).filter(
                JobAnalytics.job_id == job_id
            ).first()
            
            if existing_analytics:
                # Analytics already exist - idempotent operation
                return True
            
            # Get all logs for this job
            logs = db.query(SparkEventLog).filter(
                SparkEventLog.job_id == job_id
            ).all()
            
            if not logs:
                return False
            
            # Extract job details
            logs_by_type = {log.event_type: log for log in logs}
            
            # Check if we have the required events
            if (SparkEventType.JOB_START.value not in logs_by_type or 
                SparkEventType.JOB_END.value not in logs_by_type):
                return False
            
            # Get job start and end events
            job_start = logs_by_type[SparkEventType.JOB_START.value]
            job_end = logs_by_type[SparkEventType.JOB_END.value]
            
            # Get task events
            task_events = [log for log in logs if log.event_type == SparkEventType.TASK_END.value]
            
            # Calculate job metrics
            start_time = job_start.timestamp
            end_time = job_end.timestamp
            duration_seconds = (end_time - start_time).total_seconds()
            
            # Task statistics
            task_count = len(task_events)
            failed_tasks = sum(1 for task in task_events if not task.payload.get("successful", False))
            success_rate = 0 if task_count == 0 else ((task_count - failed_tasks) / task_count) * 100
            
            # Create analytics entry
            job_result = job_end.payload.get("job_result", JobResult.UNKNOWN.value)
            analytics = JobAnalytics(
                job_id=job_id,
                user=job_start.user,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration_seconds,
                task_count=task_count,
                failed_tasks=failed_tasks,
                success_rate=success_rate,
                job_result=job_result
            )
            
            # Save to database
            db.add(analytics)
            db.commit()
            
            # Clear the cache for this job and date
            AnalyticsService.clear_cache_for_job(job_id, start_time.date())
            
            return True
            
        except Exception as e:
            db.rollback()
            raise e
    
    @staticmethod
    def get_job_analytics(db: Session, job_id: int) -> Optional[JobAnalytics]:
        """
        Get analytics for a specific job.
        
        Args:
            db: Database session
            job_id: Job ID to get analytics for
            
        Returns:
            JobAnalytics instance or None if not found
        """
        # Try to get from cache first
        cache_key = f"job_analytics:{job_id}"
        cached_data = redis_client.get(cache_key)
        
        if cached_data:
            # Restore from cache
            return JobAnalytics(**cached_data)
        
        # Get from database
        analytics = db.query(JobAnalytics).filter(
            JobAnalytics.job_id == job_id
        ).first()
        
        if analytics:
            # Cache for future requests
            redis_client.set(cache_key, analytics.to_dict())
            
        return analytics
    
    @staticmethod
    def get_daily_summary(db: Session, date_str: str) -> Dict[str, Any]:
        """
        Get summary analytics for all jobs completed on a specific date.
        
        Args:
            db: Database session
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            Dictionary with summary statistics
        """
        # Parse date
        try:
            target_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("Invalid date format. Use YYYY-MM-DD")
        
        # Try to get from cache first
        cache_key = f"daily_summary:{date_str}"
        cached_data = redis_client.get(cache_key)
        
        if cached_data:
            # Return cached data
            return cached_data
        
        # Get all jobs completed on the target date
        jobs = db.query(JobAnalytics).filter(
            cast(JobAnalytics.end_time, Date) == target_date
        ).all()
        
        if not jobs:
            # No jobs found for this date
            return {
                "date": target_date.isoformat(),
                "total_jobs": 0,
                "avg_duration_seconds": 0,
                "avg_success_rate": 0,
                "jobs": []
            }
        
        # Calculate summary statistics
        total_jobs = len(jobs)
        avg_duration = sum(job.duration_seconds for job in jobs) / total_jobs
        avg_success_rate = sum(job.success_rate for job in jobs) / total_jobs
        
        # Create summary
        summary = {
            "date": target_date.isoformat(),
            "total_jobs": total_jobs,
            "avg_duration_seconds": round(avg_duration, 2),
            "avg_success_rate": round(avg_success_rate, 2),
            "jobs": [
                {
                    "job_id": job.job_id,
                    "user": job.user,
                    "duration_seconds": round(job.duration_seconds, 2),
                    "task_count": job.task_count,
                    "success_rate": round(job.success_rate, 2),
                    "job_result": job.job_result
                }
                for job in jobs
            ]
        }
        
        # Cache the summary
        redis_client.set(cache_key, summary)
        
        return summary
    
    @staticmethod
    def clear_cache_for_job(job_id: int, date: datetime.date) -> None:
        """
        Clear cache entries related to a job.
        
        Args:
            job_id: Job ID
            date: Job date
        """
        # Clear job-specific cache
        redis_client.delete(f"job_analytics:{job_id}")
        
        # Clear daily summary cache
        redis_client.delete(f"daily_summary:{date.isoformat()}") 