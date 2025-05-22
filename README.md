# Spark Event Logs Analytics Service

A scalable backend service for ingesting, processing, and analyzing Apache Spark event logs.

## Features

- **Asynchronous Processing**: Events are ingested quickly through the API and processed in background workers
- **Real-time Analytics**: Analytics are computed automatically as logs are processed
- **Robust Design**: Handles out-of-order events, duplicates, and incomplete data sets
- **Caching**: Uses Redis to cache frequently accessed analytics for faster responses
- **Containerization**: Full Docker setup for easy deployment
- **Comprehensive API**: RESTful API for log ingestion and analytics retrieval

## Architecture

This application is built with a microservices architecture:

- **FastAPI Backend**: Provides RESTful API endpoints
- **PostgreSQL**: Persistent storage for logs and analytics
- **Redis**: Caching for analytics results
- **RabbitMQ**: Message broker for task queue
- **Celery**: Background task processing
- **Docker**: Containerization for all components

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/BatukeshwarVats/analytics.git
   cd analytics
   ```

2. Copy the example environment file:
   ```
   cp .env.example .env
   ```

3. Start the services using Docker Compose:
   ```
   docker-compose up -d
   ```

4. The API will be available at http://localhost:8000

### API Documentation

Once the service is running, you can access the Swagger UI at http://localhost:8000/docs for interactive API documentation.

## API Reference

### Log Ingestion API

**Endpoint**: `POST /api/v1/logs/ingest`

Accepts a JSON payload with the following format:

```json
{
  "event": "SparkListenerJobStart",
  "job_id": 101,
  "timestamp": "2024-03-30T10:12:45Z",
  "user": "data_engineer_1@example.com"
}
```

**Response**:

```json
{
  "success": true,
  "message": "Log ingested successfully",
  "log_id": 123
}
```

### Analytics API

**Endpoint**: `GET /api/v1/analytics/jobs/{job_id}`

Returns analytics for a specific job.

**Response**:

```json
{
  "job_id": 101,
  "user": "data_engineer_1@example.com",
  "start_time": "2024-03-30T10:12:45Z",
  "end_time": "2024-03-30T10:14:03Z",
  "duration_seconds": 78,
  "task_count": 12,
  "failed_tasks": 2,
  "success_rate": 83.33,
  "job_result": "JobSucceeded"
}
```

**Endpoint**: `GET /api/v1/analytics/summary?date=YYYY-MM-DD`

Returns a summary of all jobs completed on the specified date.

**Response**:

```json
{
  "date": "2024-03-30",
  "total_jobs": 5,
  "avg_duration_seconds": 67.8,
  "avg_success_rate": 92.6,
  "jobs": [
    {
      "job_id": 101,
      "user": "data_engineer_1@example.com",
      "duration_seconds": 78.0,
      "task_count": 12,
      "success_rate": 83.33,
      "job_result": "JobSucceeded"
    },
    // ... more jobs
  ]
}
```

## Testing with Sample Data

### Generating Sample Logs

To generate sample Spark event logs for testing:

```
python scripts/generate_sample_logs.py --jobs 10 --days 5 --output sample_logs.json
```

This will create a file `sample_logs.json` with random Spark events for 10 jobs spread over the last 5 days.

### Ingesting Sample Logs

To ingest the sample logs into the service:

```
python scripts/ingest_sample_logs.py --file sample_logs.json --delay 0.5
```

Options:
- `--file`: Path to the JSON file with sample logs
- `--url`: API URL (default: http://localhost:8000/api/v1/logs/ingest)
- `--delay`: Delay between requests in seconds (default: 0.5)
- `--no-shuffle`: Don't shuffle the logs (ingest in file order)

## Development

### Local Setup

1. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Run the application:
   ```
   uvicorn app.main:app --reload
   ```

### Running Tests

```
pytest
```

## Project Structure

```
.
├── app/                    # Application code
│   ├── api/                # API endpoints
│   ├── core/               # Core configuration
│   ├── database/           # Database models and sessions
│   ├── models/             # SQLAlchemy models
│   ├── schemas/            # Pydantic schemas
│   ├── services/           # Business logic
│   ├── utils/              # Utility functions
│   └── workers/            # Celery tasks
├── scripts/                # Utility scripts
├── tests/                  # Test cases
├── .env.example            # Example environment variables
├── docker-compose.yml      # Docker services configuration
├── Dockerfile              # Docker build instructions
├── requirements.txt        # Python dependencies
└── README.md               # This file
```

## Design Decisions and Assumptions

1. **Event Handling**: The system assumes that each job has one JobStart and one JobEnd event, but multiple TaskEnd events. Events may arrive out of order.

2. **Deduplication**: Events with the same job_id, event_type, and timestamp are considered duplicates and will not be re-ingested.

3. **Processing Logic**: A job is only processed when both the start and end events are available. Tasks reported after job completion are still included in analytics.

4. **Caching Strategy**: Job-specific analytics are cached indefinitely until the job is updated. Daily summaries are cached but refreshed when new jobs are processed for that date.

5. **Error Handling**: Failed tasks in a job don't prevent the job from being processed. Error information is stored for debugging.