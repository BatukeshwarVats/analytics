#!/usr/bin/env python3
"""
Script to generate sample Spark event logs for testing.

This script generates a set of sample Spark event logs for testing
the Spark Analytics service. The logs are saved to a JSON file that
can be ingested using the ingest_sample_logs.py script.
"""
import argparse
import sys
import os

# Add the parent directory to sys.path so we can import the app modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.utils.sample_logs import save_sample_logs


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Generate sample Spark event logs")
    parser.add_argument("--output", "-o", default="sample_logs.json", help="Output file path")
    parser.add_argument("--jobs", "-j", type=int, default=10, help="Number of jobs to generate")
    parser.add_argument("--days", "-d", type=int, default=5, help="Number of days in the past to spread the jobs")
    
    args = parser.parse_args()
    
    # Generate sample logs
    save_sample_logs(args.output, args.jobs, args.days)


if __name__ == "__main__":
    main() 