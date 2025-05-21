import json
from typing import Any, Optional
import redis
from app.core.config import settings


class RedisClient:
    """Redis client for caching analytics results."""
    
    def __init__(self):
        self.redis_client = redis.from_url(str(settings.REDIS_URL))
        self.ttl = settings.CACHE_TTL_SECONDS
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from Redis."""
        data = self.redis_client.get(key)
        if data:
            return json.loads(data)
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set a value in Redis with an optional TTL (time to live)."""
        ttl = ttl if ttl is not None else self.ttl
        self.redis_client.setex(
            name=key,
            time=ttl,
            value=json.dumps(value),
        )
    
    def delete(self, key: str) -> None:
        """Delete a key from Redis."""
        self.redis_client.delete(key)
    
    def flush_all(self) -> None:
        """Clear all data from Redis (use with caution)."""
        self.redis_client.flushall()


# Create a singleton instance
redis_client = RedisClient() 