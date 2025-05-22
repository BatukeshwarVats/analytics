import os
from typing import Any, Dict, Optional
from pydantic import PostgresDsn, RedisDsn, validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    
    Uses Pydantic's settings management to validate and provide defaults.
    """
    model_config = SettingsConfigDict(case_sensitive=True, env_file=".env")
    
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Spark Analytics Service"
    
    DATABASE_URL: PostgresDsn
    REDIS_URL: RedisDsn
    CELERY_BROKER_URL: str
    CELERY_RESULT_BACKEND: str
    
    PROCESS_INTERVAL_SECONDS: int = 60
    CACHE_TTL_SECONDS: int = 3600  # 1 hour
    
    @validator("DATABASE_URL", pre=True)
    def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        return PostgresDsn.build(
            scheme="postgresql",
            username="postgres",
            password="postgres",
            host="db",
            path="spark_analytics",
        )
    
    @validator("REDIS_URL", pre=True)
    def assemble_redis_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        return RedisDsn.build(
            scheme="redis",
            host="redis", 
            port="6379",
            path="0",
        )


settings = Settings() 