from enum import Enum, auto


class SparkEventType(str, Enum):
    """Types of Spark events that can be ingested."""
    JOB_START = "SparkListenerJobStart"
    TASK_END = "SparkListenerTaskEnd"
    JOB_END = "SparkListenerJobEnd"


class ProcessingStatus(str, Enum):
    """Status of event log processing."""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


class JobResult(str, Enum):
    """Possible results of a Spark job."""
    SUCCEEDED = "JobSucceeded"
    FAILED = "JobFailed"
    UNKNOWN = "Unknown" 