import json
import random
import datetime
import uuid
from typing import List, Dict, Any

# Sample users
USERS = [
    "data_engineer_1@example.com",
    "data_scientist_2@example.com",
    "ml_engineer@example.com",
    "analyst@example.com",
    "data_ops@example.com"
]

# Sample job results
JOB_RESULTS = ["JobSucceeded", "JobFailed"]

def format_timestamp(dt: datetime.datetime) -> str:
    """Format a datetime object to ISO 8601 format with Z suffix for UTC."""
    # Convert to UTC, remove timezone info and add Z suffix
    if dt.tzinfo is not None:
        dt = dt.astimezone(datetime.timezone.utc)
        # Format as ISO 8601 without timezone info, then add Z
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    else:
        # If no timezone, assume UTC
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

def generate_job_start(job_id: int, timestamp: datetime.datetime, user: str) -> Dict[str, Any]:
    """Generate a SparkListenerJobStart event."""
    return {
        "event": "SparkListenerJobStart",
        "job_id": job_id,
        "timestamp": format_timestamp(timestamp),
        "user": user
    }

def generate_task_end(job_id: int, timestamp: datetime.datetime, user: str, task_id: str, success: bool = True) -> Dict[str, Any]:
    """Generate a SparkListenerTaskEnd event."""
    return {
        "event": "SparkListenerTaskEnd",
        "job_id": job_id,
        "timestamp": format_timestamp(timestamp),
        "user": user,
        "task_id": task_id,
        "duration_ms": random.randint(500, 15000),
        "successful": success
    }

def generate_job_end(job_id: int, timestamp: datetime.datetime, user: str, success: bool = True) -> Dict[str, Any]:
    """Generate a SparkListenerJobEnd event."""
    return {
        "event": "SparkListenerJobEnd",
        "job_id": job_id,
        "timestamp": format_timestamp(timestamp),
        "user": user,
        "completion_time": format_timestamp(timestamp),
        "job_result": "JobSucceeded" if success else "JobFailed"
    }

def generate_job_events(job_id: int, start_time: datetime.datetime, duration_minutes: int, 
                        user: str, num_tasks: int, failure_rate: float = 0.1) -> List[Dict[str, Any]]:
    """
    Generate a complete set of events for a single job.
    
    Args:
        job_id: Unique job ID
        start_time: Start time of the job
        duration_minutes: Duration of the job in minutes
        user: User email
        num_tasks: Number of tasks in the job
        failure_rate: Percentage of tasks that should fail
        
    Returns:
        List of event dictionaries
    """
    events = []
    
    # Generate job start event
    events.append(generate_job_start(job_id, start_time, user))
    
    # Generate task events
    end_time = start_time + datetime.timedelta(minutes=duration_minutes)
    task_interval = (end_time - start_time) / num_tasks
    
    failed_tasks = 0
    for i in range(num_tasks):
        task_time = start_time + task_interval * i
        task_id = f"task_{job_id}_{i:03d}"
        task_success = random.random() > failure_rate
        events.append(generate_task_end(job_id, task_time, user, task_id, task_success))
        if not task_success:
            failed_tasks += 1
    
    # Determine job success based on failed tasks
    job_success = failed_tasks / num_tasks < 0.5 if num_tasks > 0 else True
    
    # Generate job end event
    events.append(generate_job_end(job_id, end_time, user, job_success))
    
    # Randomize event order (to simulate out-of-order arrival)
    random.shuffle(events)
    
    return events

def generate_sample_logs(num_jobs: int = 5, days_back: int = 3) -> List[Dict[str, Any]]:
    """
    Generate sample logs for multiple jobs.
    
    Args:
        num_jobs: Number of jobs to generate
        days_back: Number of days in the past to spread the jobs
        
    Returns:
        List of event dictionaries
    """
    all_events = []
    start_job_id = 100
    for i in range(num_jobs):
        job_id = start_job_id + i
        user = random.choice(USERS)
        days_ago = random.randint(0, days_back)
        hours_ago = random.randint(0, 23)
        start_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days_ago, hours=hours_ago)
        duration = random.randint(1, 60)  # 1-60 minutes
        num_tasks = random.randint(5, 20)
        failure_rate = random.random() * 0.3  # 0-30% failure rate
        
        job_events = generate_job_events(job_id, start_time, duration, user, num_tasks, failure_rate)
        all_events.extend(job_events)
    
    return all_events

def save_sample_logs(filename: str, num_jobs: int = 5, days_back: int = 3) -> None:
    """
    Generate and save sample logs to a file.
    
    Args:
        filename: File to save the logs to
        num_jobs: Number of jobs to generate
        days_back: Number of days in the past to spread the jobs
    """
    logs = generate_sample_logs(num_jobs, days_back)
    with open(filename, 'w') as f:
        json.dump(logs, f, indent=2)
    print(f"Generated {len(logs)} events for {num_jobs} jobs and saved to {filename}")

if __name__ == "__main__":
    save_sample_logs("sample_logs.json", num_jobs=10, days_back=5) 