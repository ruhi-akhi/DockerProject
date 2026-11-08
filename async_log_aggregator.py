
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path






class AsyncLogAggregator:
    def __init__(self, filename="app_logs.json"):
        self.filename = Path(filename)
        self.queue = asyncio.Queue()
        self.logs = []
        self.worker_task = None
        self.running = False

    async def start(self):
        if self.running:
            return

        self.running = True
        self.worker_task = asyncio.create_task(self._worker())
        print("Log aggregator started.")

    async def log(self, level, message, service="app"):
        if not self.running:
            raise RuntimeError("Start the aggregator first.")

        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level.upper(),
            "service": service,
            "message": message,
        }

        await self.queue.put(entry)

    async def _worker(self):
        while True:
            entry = await self.queue.get()

            try:
                if entry is None:
                    return

                self.logs.append(entry)

                print(
                    f"[{entry['level']}] "
                    f"[{entry['service']}] "
                    f"{entry['message']}"
                )

            finally:
                self.queue.task_done()

    async def flush(self):
        await self.queue.join()

        with self.filename.open("w", encoding="utf-8") as file:
            json.dump(self.logs, file, indent=4)

        print(f"\nSaved {len(self.logs)} logs to {self.filename}")

    async def shutdown(self):
        if not self.running:
            return

        await self.queue.join()
        await self.queue.put(None)

        await self.worker_task

        self.running = False
        await self.flush()

        print("Log aggregator stopped.")

    def filter_logs(self, level=None, service=None):
        results = self.logs

        if level:
            results = [
                log for log in results
                if log["level"] == level.upper()
            ]

        if service:
            results = [
                log for log in results
                if log["service"] == service
            ]

        return results


async def application_service(name, aggregator):
    await aggregator.log(
        "INFO",
        f"{name} service started",
        service=name,
    )

    await asyncio.sleep(0.3)

    await aggregator.log(
        "WARNING",
        f"{name} response time is high",
        service=name,
    )

    await asyncio.sleep(0.2)

    await aggregator.log(
        "ERROR",
        f"{name} connection failed",
        service=name,
    )


async def main():
    aggregator = AsyncLogAggregator()

    await aggregator.start()

    # Run multiple services concurrently
    await asyncio.gather(
        application_service("auth", aggregator),
        application_service("payment", aggregator),
        application_service("notification", aggregator),
    )

    await aggregator.flush()

    # Filter error logs
    print("\n--- ERROR LOGS ---")

    errors = aggregator.filter_logs(level="ERROR")

    for error in errors:
        print(error)

    # Filter a specific service
    print("\n--- PAYMENT LOGS ---")

    payment_logs = aggregator.filter_logs(service="payment")

    for log in payment_logs:
        print(log)

    await aggregator.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

