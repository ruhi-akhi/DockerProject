
import asyncio
from dataclasses import dataclass
from typing import Any


@dataclass
class WebhookJob:
    event_id: str
    endpoint: str
    payload: dict[str, Any]
    attempts: int = 0


class WebhookDispatcher:
    def __init__(
        self,
        workers=3,
        max_retries=3,
        retry_delay=1,
    ):
        self.queue = asyncio.Queue()
        self.worker_count = workers
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.dead_letters = []
        self.delivered = []
        self.running = False
        self.tasks = []

    async def start(self):
        if self.running:
            return

        self.running = True

        for worker_id in range(self.worker_count):
            task = asyncio.create_task(
                self.worker(worker_id + 1)
            )
            self.tasks.append(task)

        print("Webhook dispatcher started.")

    async def dispatch(self, event_id, endpoint, payload):
        if not self.running:
            raise RuntimeError("Dispatcher is not running.")

        job = WebhookJob(
            event_id=event_id,
            endpoint=endpoint,
            payload=payload,
        )

        await self.queue.put(job)

        print(f"[QUEUED] {event_id}")

    async def send_webhook(self, job):
        # Simulate network request
        await asyncio.sleep(0.3)

        # This endpoint always fails
        if job.endpoint == "https://bad.example.com":
            raise ConnectionError("Endpoint unavailable")

        # This endpoint fails twice, then succeeds
        if job.event_id == "payment_001" and job.attempts < 2:
            raise TimeoutError("Request timed out")

        return {"status": 200, "message": "Delivered"}

    async def worker(self, worker_id):
        print(f"Worker {worker_id} started.")

        while True:
            job = await self.queue.get()

            try:
                if job is None:
                    return

                while job.attempts < self.max_retries:
                    job.attempts += 1

                    try:
                        print(
                            f"[WORKER {worker_id}] "
                            f"Sending {job.event_id}, "
                            f"attempt {job.attempts}"
                        )

                        response = await self.send_webhook(job)

                        self.delivered.append({
                            "event_id": job.event_id,
                            "response": response,
                            "attempts": job.attempts,
                        })

                        print(
                            f"[SUCCESS] {job.event_id} "
                            f"after {job.attempts} attempt(s)"
                        )
                        break

                    except Exception as error:
                        print(
                            f"[FAILED] {job.event_id}: {error}"
                        )

                        if job.attempts >= self.max_retries:
                            self.dead_letters.append({
                                "event_id": job.event_id,
                                "endpoint": job.endpoint,
                                "payload": job.payload,
                                "error": str(error),
                                "attempts": job.attempts,
                            })

                            print(
                                f"[DEAD LETTER] {job.event_id}"
                            )
                            break

                        delay = (
                            self.retry_delay
                            * (2 ** (job.attempts - 1))
                        )

                        print(f"[RETRY] Waiting {delay}s...")
                        await asyncio.sleep(delay)

            finally:
                self.queue.task_done()

    async def shutdown(self):
        if not self.running:
            return

        await self.queue.join()

        for _ in self.tasks:
            await self.queue.put(None)

        await asyncio.gather(*self.tasks)

        self.tasks.clear()
        self.running = False

        print("\nDispatcher stopped.")

    def show_report(self):
        print("\n========== REPORT ==========")
        print("Delivered:", len(self.delivered))
        print("Dead letters:", len(self.dead_letters))

        print("\nFailed Events:")

        for item in self.dead_letters:
            print(
                item["event_id"],
                "| Attempts:",
                item["attempts"],
            )


async def main():
    dispatcher = WebhookDispatcher(
        workers=3,
        max_retries=3,
        retry_delay=1,
    )

    await dispatcher.start()

    # Successful event
    await dispatcher.dispatch(
        "user_001",
        "https://api.example.com/users",
        {"user": "Akhi", "action": "registered"},
    )

    # Fails twice, then succeeds
    await dispatcher.dispatch(
        "payment_001",
        "https://api.example.com/payments",
        {"amount": 1500, "status": "paid"},
    )

    # Always fails and enters dead letter queue
    await dispatcher.dispatch(
        "order_001",
        "https://bad.example.com",
        {"order_id": 1001, "status": "confirmed"},
    )

    await dispatcher.shutdown()

    dispatcher.show_report()


if __name__ == "__main__":
    asyncio.run(main())
