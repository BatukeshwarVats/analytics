from celery import Celery
from celery.schedules import crontab
from app.core.config import settings

# Initialize Celery app with RabbitMQ broker and Redis backend
celery_app = Celery(
    "worker",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

# Configure Celery
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    worker_prefetch_multiplier=1,  # Process one task at a time for better resource management
    task_acks_late=True,  # Only acknowledge tasks after they're processed (important for fault tolerance)
    task_reject_on_worker_lost=True,  # Reject tasks if the worker crashes (allows for retries)
)

# Configure the periodic tasks with Celery Beat
celery_app.conf.beat_schedule = {
    "process-spark-logs-every-minute": {
        "task": "app.workers.tasks.process_unprocessed_logs",
        "schedule": settings.PROCESS_INTERVAL_SECONDS,
    },
}

# Add proper module for task autodiscovery
celery_app.autodiscover_tasks(["app.workers"]) 