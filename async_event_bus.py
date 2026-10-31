
import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any


Handler = Callable[..., Awaitable[None]]


class AsyncEventBus:
    def __init__(self):
        self._handlers: dict[str, list[Handler]] = defaultdict(list)

    def subscribe(self, event_name: str, handler: Handler) -> None:
        """Register a handler for an event."""
        if handler not in self._handlers[event_name]:
            self._handlers[event_name].append(handler)

    def unsubscribe(self, event_name: str, handler: Handler) -> None:
        """Remove a registered handler."""
        handlers = self._handlers.get(event_name, [])

        if handler in handlers:
            handlers.remove(handler)

    async def publish(self, event_name: str, **data: Any) -> None:
        """Notify all handlers registered for this event."""
        handlers = list(self._handlers.get(event_name, []))

        if not handlers:
            print(f"No handlers registered for '{event_name}'")
            return

        results = await asyncio.gather(
            *(handler(**data) for handler in handlers),
            return_exceptions=True,
        )

        for handler, result in zip(handlers, results):
            if isinstance(result, Exception):
                print(
                    f"Handler {handler.__name__} failed: {result}"
                )


# Example event handlers
async def send_email(user: str, action: str) -> None:
    await asyncio.sleep(0.5)
    print(f"[EMAIL] {user}: {action}")


async def update_analytics(user: str, action: str) -> None:
    await asyncio.sleep(0.2)
    print(f"[ANALYTICS] {user} performed {action}")


async def update_dashboard(user: str, action: str) -> None:
    await asyncio.sleep(0.1)
    print(f"[DASHBOARD] Updated for {user}")


async def main():
    bus = AsyncEventBus()

    # Subscribe handlers
    bus.subscribe("user_action", send_email)
    bus.subscribe("user_action", update_analytics)
    bus.subscribe("user_action", update_dashboard)

    print("Publishing event...\n")

    # All handlers run concurrently
    await bus.publish(
        "user_action",
        user="Akhi",
        action="login",
    )

    print("\nRemoving dashboard handler...")
    bus.unsubscribe("user_action", update_dashboard)

    print("\nPublishing another event...\n")

    await bus.publish(
        "user_action",
        user="Akhi",
        action="purchase",
    )


if __name__ == "__main__":
    asyncio.run(main())
