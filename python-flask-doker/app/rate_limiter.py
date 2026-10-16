
import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass


@dataclass
class RateLimitResult:
    allowed: bool
    remaining: int
    retry_after: float = 0.0


class SlidingWindowRateLimiter:
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds

        # Store request timestamps for each user
        self.requests = defaultdict(deque)

        # TODO 1: Create a lock for concurrent access
        self.lock = None

    async def allow_request(self, user_id: str) -> RateLimitResult:
        async with self.lock:
            now = time.monotonic()
            timestamps = self.requests[user_id]

            # TODO 2: Remove timestamps outside the window
            # Keep only timestamps newer than:
            cutoff = now - self.window_seconds

            # TODO 3: If the user reached the limit,
            # return allowed=False and calculate retry_after

            # TODO 4: Otherwise, record this request
            # and return allowed=True with remaining requests

            raise NotImplementedError


async def simulate_requests():
    limiter = SlidingWindowRateLimiter(
        max_requests=5,
        window_seconds=10
    )

    async def send_request(user_id: str, request_id: int):
        result = await limiter.allow_request(user_id)

        print(
            f"User={user_id} Request={request_id} "
            f"Allowed={result.allowed} "
            f"Remaining={result.remaining} "
            f"Retry={result.retry_after:.2f}s"
        )

    # Send 10 concurrent requests for the same user
    await asyncio.gather(
        *(send_request("user-123", i) for i in range(10))
    )


if __name__ == "__main__":
    asyncio.run(simulate_requests())
