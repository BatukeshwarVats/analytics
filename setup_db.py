import os
import sys
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app.database.base import Base, engine
from app.models import log, analytics  # Import all models to ensure they're registered with Base.metadata

def setup_database():
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("Database tables created successfully.")

if __name__ == "__main__":
    setup_database()