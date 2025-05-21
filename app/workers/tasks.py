import logging
from typing import List

from app.core.celery_app import celery_app
from app.database.base import SessionLocal
from app.core.enums import ProcessingStatus
from app.services.log_service import LogService
from app.services.analytics_service import AnalyticsService

logger = logging.getLogger(__name__)


@celery_app.task(name="app.workers.tasks.process_unprocessed_logs")
def process_unprocessed_logs(batch_size: int = 50) -> dict:
    """
    Process unprocessed logs in batch.
    
    This task performs the following steps:
    1. Get a batch of job IDs with unprocessed logs
    2. For each job ID, check if the job is ready for processing
    3. If ready, process the job and update log status
    
    Args:
        batch_size: Maximum number of jobs to process in one batch
        
    Returns:
        Dictionary with processing statistics
    """
    db = SessionLocal()
    try:
        processed_jobs = 0
        failed_jobs = 0
        skipped_jobs = 0
        
        # Get job IDs with unprocessed logs
        job_ids = LogService.get_unprocessed_job_logs(db, batch_size)
        
        if not job_ids:
            logger.info("No unprocessed logs found")
            return {
                "processed": 0,
                "failed": 0,
                "skipped": 0,
                "total": 0
            }
            
        logger.info(f"Processing logs for {len(job_ids)} jobs")
        
        # Process each job
        for job_id in job_ids:
            # Mark logs as processing
            LogService.update_logs_status(db, job_id, ProcessingStatus.PROCESSING)
            
            # Check if job is ready for processing
            if not LogService.check_job_readiness(db, job_id):
                logger.info(f"Job {job_id} is not ready for processing yet")
                skipped_jobs += 1
                # Reset to pending since we can't process it yet
                LogService.update_logs_status(db, job_id, ProcessingStatus.PENDING)
                continue
                
            try:
                # Process job analytics
                success = AnalyticsService.process_job(db, job_id)
                
                if success:
                    # Mark logs as processed
                    LogService.update_logs_status(db, job_id, ProcessingStatus.PROCESSED)
                    processed_jobs += 1
                    logger.info(f"Successfully processed job {job_id}")
                else:
                    # Mark logs as failed
                    LogService.update_logs_status(
                        db, 
                        job_id, 
                        ProcessingStatus.FAILED,
                        "Failed to process job analytics"
                    )
                    failed_jobs += 1
                    logger.error(f"Failed to process job {job_id}")
                    
            except Exception as e:
                # Mark logs as failed
                LogService.update_logs_status(
                    db, 
                    job_id, 
                    ProcessingStatus.FAILED,
                    str(e)
                )
                failed_jobs += 1
                logger.exception(f"Error processing job {job_id}: {str(e)}")
        
        return {
            "processed": processed_jobs,
            "failed": failed_jobs,
            "skipped": skipped_jobs,
            "total": len(job_ids)
        }
        
    finally:
        db.close()


@celery_app.task(name="app.workers.tasks.process_specific_job")
def process_specific_job(job_id: int) -> dict:
    """
    Process a specific job.
    
    This task processes a single job regardless of its current status.
    
    Args:
        job_id: ID of the job to process
        
    Returns:
        Dictionary with processing result
    """
    db = SessionLocal()
    try:
        # Mark logs as processing
        LogService.update_logs_status(db, job_id, ProcessingStatus.PROCESSING)
        
        # Check if job is ready for processing
        if not LogService.check_job_readiness(db, job_id):
            logger.info(f"Job {job_id} is not ready for processing yet")
            # Reset to pending since we can't process it yet
            LogService.update_logs_status(db, job_id, ProcessingStatus.PENDING)
            return {
                "job_id": job_id,
                "success": False,
                "message": "Job is not ready for processing (missing required logs)"
            }
            
        try:
            # Process job analytics
            success = AnalyticsService.process_job(db, job_id)
            
            if success:
                # Mark logs as processed
                LogService.update_logs_status(db, job_id, ProcessingStatus.PROCESSED)
                logger.info(f"Successfully processed job {job_id}")
                return {
                    "job_id": job_id,
                    "success": True,
                    "message": "Job processed successfully"
                }
            else:
                # Mark logs as failed
                LogService.update_logs_status(
                    db, 
                    job_id, 
                    ProcessingStatus.FAILED,
                    "Failed to process job analytics"
                )
                logger.error(f"Failed to process job {job_id}")
                return {
                    "job_id": job_id,
                    "success": False,
                    "message": "Failed to process job analytics"
                }
                
        except Exception as e:
            # Mark logs as failed
            LogService.update_logs_status(
                db, 
                job_id, 
                ProcessingStatus.FAILED,
                str(e)
            )
            logger.exception(f"Error processing job {job_id}: {str(e)}")
            return {
                "job_id": job_id,
                "success": False,
                "message": f"Error: {str(e)}"
            }
    
    finally:
        db.close() 