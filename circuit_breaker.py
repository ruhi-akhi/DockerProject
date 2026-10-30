
import asyncio
import time
from enum import Enum
from typing import Any, Awaitable, Callable


class State(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_timeout: float = 5.0,
    ):
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")

        if recovery_timeout < 0:
            raise ValueError("recovery_timeout cannot be negative")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self.state = State.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self._lock = asyncio.Lock()

    async def call(
        self,
        func: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        async with self._lock:
            if self.state == State.OPEN:
                elapsed = time.monotonic() - self.last_failure_time

                if elapsed < self.recovery_timeout:
                    raise RuntimeError(
                        "Circuit is OPEN. Request blocked."
                    )

                self.state = State.HALF_OPEN
                print("Circuit: HALF_OPEN")

        try:
            result = await func(*args, **kwargs)

        except Exception:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.monotonic()

                if (
                    self.state == State.HALF_OPEN
                    or self.failure_count >= self.failure_threshold
                ):
                    self.state = State.OPEN
                    print("Circuit: OPEN")

            raise

        async with self._lock:
            self.failure_count = 0
            self.state = State.CLOSED
            print("Circuit: CLOSED")

        return result


# Simulated external API
async def external_api(should_fail: bool) -> str:
    await asyncio.sleep(0.3)

    if should_fail:
        raise ConnectionError("External API is unavailable")

    return "API response received successfully"


async def main():
    breaker = CircuitBreaker(
        failure_threshold=3,
        recovery_timeout=3,
    )

    # Cause three failures to open the circuit
    for attempt in range(1, 4):
        try:
            await breaker.call(external_api, True)
        except Exception as error:
            print(f"Attempt {attempt}: {error}")

    # This request should be blocked
    try:
        await breaker.call(external_api, False)
    except Exception as error:
        print(f"Blocked request: {error}")

    # Wait until recovery is allowed
    print("\nWaiting for recovery...")
    await asyncio.sleep(3.2)

    # Successful request closes the circuit
    try:
        result = await breaker.call(external_api, False)
        print("Result:", result)
        print("Final state:", breaker.state.value)
    except Exception as error:
        print("Recovery failed:", error)


if __name__ == "__main__":
    asyncio.run(main())
