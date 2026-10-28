
import asyncio
import heapq
import itertools
import json
import time
import uuid

from pathlib import Path


class AsyncJobQueue:
    def __init__(
        self,
        workers=3,
        state_file="job_state.json",
    ):
        if workers < 1:
            raise ValueError("Workers must be at least 1")

        self.worker_count = workers
        self.state_file = Path(state_file)

        self.queue = []
        self.sequence = itertools.count()
        self.condition = asyncio.Condition()

        self.pending = set()
        self.active = 0
        self.closed = False

        self.status = {}
        self.dead_letters = []

        self.handlers = {
            "send_email": self.send_email,
            "generate_report": self.generate_report,
            "process_payment": self.process_payment,
        }

    async def submit(
        self,
        name,
        payload,
        priority=10,
        max_retries=3,
    ):
        if name not in self.handlers:
            raise ValueError(f"Unknown job type: {name}")

        if max_retries < 0:
            raise ValueError("Retries cannot be negative")

        job_id = str(uuid.uuid4())

        job = {
            "job_id": job_id,
            "name": name,
            "payload": payload,
            "priority": priority,
            "attempts": 0,
            "max_retries": max_retries,
            "created_at": time.time(),
        }

        async with self.condition:
            self.pending.add(job_id)

            heapq.heappush(
                self.queue,
                (priority, next(self.sequence), job),
            )

            self.status[job_id] = {
                "job_id": job_id,
                "name": name,
                "status": "queued",
                "attempts": 0,
                "result": None,
                "error": None,
            }

            self.save_state()
            self.condition.notify_all()

        print(f"[SUBMITTED] {name}: {job_id}")
        return job_id

    async def get_status(self, job_id):
        async with self.condition:
            result = self.status.get(job_id)
            return dict(result) if result else None

    def save_state(self):
        snapshot = {
            "jobs": self.status,
            "dead_letters": self.dead_letters,
        }

        temporary = self.state_file.with_suffix(".tmp")

        temporary.write_text(
            json.dumps(snapshot, indent=2, default=str),
            encoding="utf-8",
        )

        temporary.replace(self.state_file)

    async def next_job(self):
        async with self.condition:
            while not self.queue and not self.closed:
                await self.condition.wait()

            if self.closed and not self.queue:
                return None

            _, _, job = heapq.heappop(self.queue)

            self.active += 1

            self.status[job["job_id"]]["status"] = "running"
            self.status[job["job_id"]]["attempts"] = (
                job["attempts"] + 1
            )

            self.save_state()
            return job

    async def worker(self, worker_id):
        while True:
            job = await self.next_job()

            if job is None:
                return

            job_id = job["job_id"]

            try:
                handler = self.handlers[job["name"]]
                result = await handler(job["payload"])

                async with self.condition:
                    self.status[job_id]["status"] = "completed"
                    self.status[job_id]["result"] = result
                    self.status[job_id]["error"] = None

                    self.pending.discard(job_id)
                    self.active -= 1

                    self.save_state()
                    self.condition.notify_all()

                print(
                    f"[WORKER {worker_id}] "
                    f"COMPLETED: {job['name']}"
                )

            except asyncio.CancelledError:
                async with self.condition:
                    self.status[job_id]["status"] = "interrupted"
                    self.status[job_id]["error"] = "Worker cancelled"

                    self.pending.discard(job_id)
                    self.active -= 1

                    self.save_state()
                    self.condition.notify_all()

                raise

            except Exception as error:
                job["attempts"] += 1

                message = f"{type(error).__name__}: {error}"

                async with self.condition:
                    self.active -= 1

                    self.status[job_id]["error"] = message
                    self.status[job_id]["attempts"] = job["attempts"]

                    if job["attempts"] > job["max_retries"]:
                        self.status[job_id]["status"] = "dead_letter"

                        self.dead_letters.append({
                            **job,
                            "error": message,
                            "failed_at": time.time(),
                        })

                        self.pending.discard(job_id)

                        print(f"[DEAD LETTER] {job['name']}")

                    else:
                        self.status[job_id]["status"] = "retrying"

                    self.save_state()
                    self.condition.notify_all()

                if job["attempts"] <= job["max_retries"]:
                    delay = min(
                        0.5 * (2 ** (job["attempts"] - 1)),
                        8,
                    )

                    print(
                        f"[RETRY] {job['name']} "
                        f"in {delay} seconds"
                    )

                    await asyncio.sleep(delay)

                    async with self.condition:
                        self.status[job_id]["status"] = "queued"

                        heapq.heappush(
                            self.queue,
                            (
                                job["priority"],
                                next(self.sequence),
                                job,
                            ),
                        )

                        self.save_state()
                        self.condition.notify_all()

    async def run_until_idle(self):
        if self.active or self.queue:
            raise RuntimeError("Queue is already running or busy")

        self.closed = False

        workers = [
            asyncio.create_task(self.worker(i + 1))
            for i in range(self.worker_count)
        ]

        try:
            async with self.condition:
                while self.pending:
                    await self.condition.wait()

                self.closed = True
                self.condition.notify_all()

            await asyncio.gather(*workers)

        finally:
            self.closed = True

            async with self.condition:
                self.condition.notify_all()

            for worker in workers:
                if not worker.done():
                    worker.cancel()

            await asyncio.gather(
                *workers,
                return_exceptions=True,
            )

    async def send_email(self, payload):
        await asyncio.sleep(0.4)

        return {
            "message": (
                f"Demo email sent to "
                f"{payload.get('to', 'unknown@example.com')}"
            )
        }

    async def generate_report(self, payload):
        await asyncio.sleep(0.8)

        return {
            "report": payload.get("report", "summary"),
            "rows": 42,
        }

    async def process_payment(self, payload):
        await asyncio.sleep(0.5)

        amount = float(payload.get("amount", 0))

        if amount <= 0:
            raise ValueError("Amount must be positive")

        return {
            "payment_status": "demo_processed",
            "amount": amount,
        }


async def main():
    queue = AsyncJobQueue(workers=3)

    email_id = await queue.submit(
        "send_email",
        {"to": "user@example.com"},
        priority=5,
    )

    report_id = await queue.submit(
        "generate_report",
        {"report": "monthly_sales"},
        priority=2,
    )

    payment_id = await queue.submit(
        "process_payment",
        {"amount": 125.50},
        priority=1,
    )

    failed_id = await queue.submit(
        "process_payment",
        {"amount": -10},
        priority=3,
        max_retries=2,
    )

    print("\nProcessing jobs...\n")

    await queue.run_until_idle()

    print("\nFINAL JOB STATUSES")

    for job_id in (
        email_id,
        report_id,
        payment_id,
        failed_id,
    ):
        print(await queue.get_status(job_id))

    print("\nDead letter count:", len(queue.dead_letters))
    print("Saved state:", queue.state_file.resolve())


if __name__ == "__main__":
    asyncio.run(main())
