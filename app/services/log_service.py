import datetime
import json
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from app.models.log import SparkEventLog
from app.core.enums import ProcessingStatus, SparkEventType


class LogService:
    """Service for managing log ingestion and retrieval."""
    
    @staticmethod
    def ingest_log(db: Session, log_data: Dict[str, Any]) -> Tuple[bool, str, Optional[int]]:
        """
        Ingest a new log entry.
        
        Args:
            db: Database session
            log_data: Log data from API
            
        Returns:
            Tuple of (success, message, log_id)
        """
        try:
            # Create a new log entry
            log_entry = SparkEventLog.from_json(log_data)
            
            # Check for duplicates (same job_id and event_type with same timestamp)
            existing_log = db.query(SparkEventLog).filter(
                and_(
                    SparkEventLog.job_id == log_entry.job_id,
                    SparkEventLog.event_type == log_entry.event_type,
                    SparkEventLog.timestamp == log_entry.timestamp
                )
            ).first()
            
            if existing_log:
                return True, f"Log entry already exists (ID: {existing_log.id})", existing_log.id
            
            # Add new log entry
            db.add(log_entry)
            db.commit()
            db.refresh(log_entry)
            
            return True, "Log ingested successfully", log_entry.id
            
        except Exception as e:
            db.rollback()
            return False, f"Error ingesting log: {str(e)}", None
    
    @staticmethod
    def get_unprocessed_job_logs(db: Session, batch_size: int = 50) -> List[int]:
        """
        Get a batch of job IDs with unprocessed logs.
        
        Args:
            db: Database session
            batch_size: Maximum number of job IDs to return
            
        Returns:
            List of job IDs with unprocessed logs
        """
        # Find job IDs with pending logs, prioritizing older jobs
        job_ids = db.query(SparkEventLog.job_id).filter(
            SparkEventLog.processing_status == ProcessingStatus.PENDING.value
        ).distinct().order_by(SparkEventLog.timestamp).limit(batch_size).all()
        
        return [job_id[0] for job_id in job_ids]
    
    @staticmethod
    def get_job_logs(db: Session, job_id: int) -> List[SparkEventLog]:
        """
        Get all logs for a specific job.
        
        Args:
            db: Database session
            job_id: Job ID to retrieve logs for
            
        Returns:
            List of SparkEventLog instances
        """
        return db.query(SparkEventLog).filter(
            SparkEventLog.job_id == job_id
        ).order_by(SparkEventLog.timestamp).all()
    
    @staticmethod
    def update_logs_status(
        db: Session, 
        job_id: int, 
        status: ProcessingStatus, 
        error_message: Optional[str] = None
    ) -> None:
        """
        Update the processing status of logs for a job.
        
        Args:
            db: Database session
            job_id: Job ID to update logs for
            status: New status to set
            error_message: Optional error message (for failed processing)
        """
        logs = db.query(SparkEventLog).filter(SparkEventLog.job_id == job_id).all()
        
        for log in logs:
            log.processing_status = status.value
            log.processing_time = datetime.datetime.utcnow()
            if error_message:
                log.error_message = error_message
        
        db.commit()
    
    @staticmethod
    def check_job_readiness(db: Session, job_id: int) -> bool:
        """
        Check if a job has all required logs for processing.
        A job is ready when both JobStart and JobEnd events are present.
        
        Args:
            db: Database session
            job_id: Job ID to check
            
        Returns:
            True if the job is ready for processing, False otherwise
        """
        # Count JobStart and JobEnd events for this job
        start_event = db.query(SparkEventLog).filter(
            and_(
                SparkEventLog.job_id == job_id,
                SparkEventLog.event_type == SparkEventType.JOB_START.value
            )
        ).first()
        
        end_event = db.query(SparkEventLog).filter(
            and_(
                SparkEventLog.job_id == job_id,
                SparkEventLog.event_type == SparkEventType.JOB_END.value
            )
        ).first()
        
        # Job is ready when both start and end events are present
        return start_event is not None and end_event is not None 