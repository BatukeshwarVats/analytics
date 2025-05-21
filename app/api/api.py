from fastapi import APIRouter
from app.api.routes import logs, analytics

api_router = APIRouter()

# Include all route modules
api_router.include_router(logs.router, prefix="/logs", tags=["logs"])
api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"]) 