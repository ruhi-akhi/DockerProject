
import asyncio
import time
from dataclasses import dataclass


@dataclass
class Bucket:
    tokens: float
    last_refill: float


class RateLimiter:
    def __init__(
        self,
        capacity: int = 5,
        refill_rate: float = 1.0,
    ):
        self.capacity = capacity
        self.refill_rate = refill_rate

        self.buckets: dict[str, Bucket] = {}
        self.locks: dict[str, asyncio.Lock] = {}

    def get_bucket(self, user_id: str) -> Bucket:
        if user_id not in self.buckets:
            self.buckets[user_id] = Bucket(
                tokens=float(self.capacity),
                last_refill=time.monotonic(),
            )

        return self.buckets[user_id]

    async def allow_request(self, user_id: str) -> bool:
        lock = self.locks.setdefault(
            user_id,
            asyncio.Lock(),
        )

        async with lock:
            bucket = self.get_bucket(user_id)
            now = time.monotonic()

            elapsed = now - bucket.last_refill

            # Refill tokens based on elapsed time
            refill = elapsed * self.refill_rate

            bucket.tokens = min(
                self.capacity,
                bucket.tokens + refill,
            )

            bucket.last_refill = now

            if bucket.tokens >= 1:
                bucket.tokens -= 1
                return True

            return False

    async def get_status(self, user_id: str):
        lock = self.locks.setdefault(
            user_id,
            asyncio.Lock(),
        )

        async with lock:
            bucket = self.get_bucket(user_id)
            now = time.monotonic()

            elapsed = now - bucket.last_refill

            bucket.tokens = min(
                self.capacity,
                bucket.tokens + elapsed * self.refill_rate,
            )

            bucket.last_refill = now

            return {
                "user_id": user_id,
                "remaining_tokens": round(bucket.tokens, 2),
                "capacity": self.capacity,
            }


# Simulated API endpoint
async def api_request(user_id, limiter, request_number):
    allowed = await limiter.allow_request(user_id)

    if allowed:
        print(
            f"[ALLOWED] User {user_id} "
            f"- Request {request_number}"
        )
    else:
        print(
            f"[BLOCKED] User {user_id} "
            f"- Request {request_number}"
        )


async def main():
    limiter = RateLimiter(
        capacity=5,
        refill_rate=1,
    )

    print("Sending 8 requests from User A...\n")

    await asyncio.gather(
        *[
            api_request("user_A", limiter, i)
            for i in range(1, 9)
        ]
    )

    print("\nUser A Status:")
    print(await limiter.get_status("user_A"))

    print("\nUser B sends a request...")

    await api_request("user_B", limiter, 1)

    print("\nWaiting 2 seconds for token refill...")
    await asyncio.sleep(2)

    print("\nUser A sends another request...")

    await api_request("user_A", limiter, 9)

    print("\nFinal Status:")
    print(await limiter.get_status("user_A"))


if __name__ == "__main__":
    asyncio.run(main())
