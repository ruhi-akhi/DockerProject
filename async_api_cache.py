
import asyncio
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable


@dataclass
class CacheEntry:
    value: Any
    expires_at: float


class AsyncAPICache:
    """Async TTL cache with concurrency-safe request deduplication."""

    def __init__(
        self,
        ttl_seconds: float = 10.0,
        max_size: int = 256,
    ):
        if ttl_seconds <= 0:
            raise ValueError("TTL must be positive")

        if max_size < 1:
            raise ValueError("max_size must be at least 1")

        self.ttl_seconds = ttl_seconds
        self.max_size = max_size

        self._cache: dict[str, CacheEntry] = {}
        self._inflight: dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

        self.hits = 0
        self.misses = 0

    async def get(
        self,
        key: str,
        loader: Callable[[], Awaitable[Any]],
    ) -> Any:

        async with self._lock:
            entry = self._cache.get(key)

            # Return valid cached data
            if entry and entry.expires_at > time.monotonic():
                self.hits += 1
                return entry.value

            # Remove expired data
            if entry:
                self._cache.pop(key, None)

            # Reuse an existing request for this key
            task = self._inflight.get(key)

            if task is None:
                self.misses += 1

                task = asyncio.create_task(
                    self._load_and_store(key, loader)
                )

                self._inflight[key] = task

        # One caller's cancellation won't cancel shared loading
        return await asyncio.shield(task)

    async def _load_and_store(
        self,
        key: str,
        loader: Callable[[], Awaitable[Any]],
    ) -> Any:

        current_task = asyncio.current_task()

        try:
            value = await loader()

            async with self._lock:
                self._remove_expired()

                # Evict the entry expiring soonest if cache is full
                if (
                    key not in self._cache
                    and len(self._cache) >= self.max_size
                ):
                    oldest_key = min(
                        self._cache,
                        key=lambda k: self._cache[k].expires_at,
                    )

                    self._cache.pop(oldest_key, None)

                self._cache[key] = CacheEntry(
                    value=value,
                    expires_at=(
                        time.monotonic() + self.ttl_seconds
                    ),
                )

            return value

        finally:
            async with self._lock:
                if self._inflight.get(key) is current_task:
                    self._inflight.pop(key, None)

    def _remove_expired(self):
        now = time.monotonic()

        expired_keys = [
            key
            for key, entry in self._cache.items()
            if entry.expires_at <= now
        ]

        for key in expired_keys:
            self._cache.pop(key, None)

    async def invalidate(self, key: str) -> bool:
        """Remove one cached entry."""

        async with self._lock:
            return self._cache.pop(key, None) is not None

    async def clear(self):
        """Clear all cached entries."""

        async with self._lock:
            self._cache.clear()

    async def stats(self):
        """Return cache statistics."""

        async with self._lock:
            total = self.hits + self.misses

            return {
                "size": len(self._cache),
                "inflight": len(self._inflight),
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": (
                    self.hits / total if total else 0.0
                ),
            }


# Simulated API request
async def fetch_user(user_id: int) -> dict:
    print(f"Fetching user {user_id} from API...")

    await asyncio.sleep(1)

    return {
        "id": user_id,
        "name": f"User {user_id}",
        "fetched_at": time.time(),
    }


async def main():
    cache = AsyncAPICache(
        ttl_seconds=5,
        max_size=100,
    )

    async def load_user():
        return await fetch_user(1)

    print("Sending 5 concurrent requests...")

    results = await asyncio.gather(
        *[
            cache.get("user:1", load_user)
            for _ in range(5)
        ]
    )

    # Only one API call should run for the same key
    for result in results:
        print(result)

    print("\nRequesting cached data...")

    cached = await cache.get("user:1", load_user)
    print(cached)

    print("\nCache statistics:")
    print(await cache.stats())

    print("\nInvalidating cache...")

    removed = await cache.invalidate("user:1")
    print("Removed:", removed)

    print("\nFetching again after invalidation...")

    result = await cache.get("user:1", load_user)
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
