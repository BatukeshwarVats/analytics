#!/usr/bin/env python3
"""
Script to ingest sample logs into the Spark Analytics service.

This script loads a JSON file containing sample Spark event logs and
sends them to the API endpoint for ingestion, simulating real-world
event ingestion.
"""
import argparse
import json
import random
import time
import sys
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

import httpx

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_sample_logs(file_path: str) -> List[Dict[str, Any]]:
    """
    Load sample logs from a JSON file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        List of log events
    """
    try:
        with open(file_path, "r") as f:
            logs = json.load(f)
        logger.info(f"Loaded {len(logs)} events from {file_path}")
        return logs
    except Exception as e:
        logger.error(f"Error loading logs from {file_path}: {str(e)}")
        sys.exit(1)


def send_log(api_url: str, log: Dict[str, Any]) -> bool:
    """
    Send a log event to the API.
    
    Args:
        api_url: API URL for log ingestion
        log: Log event to send
        
    Returns:
        True if successful, False otherwise
    """
    try:
        response = httpx.post(api_url, json=log, timeout=10.0)
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Successfully ingested log: {log['event']} for job {log['job_id']} (ID: {result.get('log_id')})")
            return True
        else:
            logger.error(f"Error ingesting log: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Exception sending log: {str(e)}")
        return False


def ingest_logs(api_url: str, logs: List[Dict[str, Any]], delay: float = 0.5, shuffle: bool = True) -> None:
    """
    Ingest a list of logs into the API.
    
    Args:
        api_url: API URL for log ingestion
        logs: List of log events to ingest
        delay: Delay between log ingestion requests (seconds)
        shuffle: Whether to shuffle the logs to simulate out-of-order ingestion
    """
    # Optionally shuffle logs
    if shuffle:
        random.shuffle(logs)
    
    success_count = 0
    failure_count = 0
    
    # Process each log
    for i, log in enumerate(logs):
        logger.info(f"Sending log {i+1}/{len(logs)}: {log['event']} for job {log['job_id']}")
        
        if send_log(api_url, log):
            success_count += 1
        else:
            failure_count += 1
        
        # Add delay between requests
        if i < len(logs) - 1 and delay > 0:
            time.sleep(delay)
    
    logger.info(f"Ingestion complete. Success: {success_count}, Failures: {failure_count}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Ingest sample logs into the Spark Analytics service")
    parser.add_argument("--file", "-f", required=True, help="Path to the JSON file containing sample logs")
    parser.add_argument("--url", "-u", default="http://localhost:8000/api/v1/logs/ingest", help="API URL for log ingestion")
    parser.add_argument("--delay", "-d", type=float, default=0.5, help="Delay between log ingestion requests (seconds)")
    parser.add_argument("--no-shuffle", action="store_true", help="Don't shuffle logs (ingest in file order)")
    
    args = parser.parse_args()
    
    # Make sure the log file exists
    if not Path(args.file).exists():
        logger.error(f"Log file does not exist: {args.file}")
        sys.exit(1)
    
    # Load sample logs
    logs = load_sample_logs(args.file)
    
    # Ingest logs
    ingest_logs(args.url, logs, args.delay, not args.no_shuffle)


if __name__ == "__main__":
    main() 