
import asyncio
import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone


class AsyncFileProcessor:
    def __init__(self, folder="uploads", workers=3):
        self.folder = Path(folder)
        self.workers = workers

        self.queue = asyncio.Queue()
        self.results = []
        self.seen_hashes = set()

        self.processed = 0
        self.failed = 0
        self.duplicates = 0

    async def setup(self):
        self.folder.mkdir(parents=True, exist_ok=True)

        # Create sample files if folder is empty
        if not any(self.folder.iterdir()):
            samples = {
                "file1.txt": "Python Backend Development",
                "file2.txt": "Async Programming",
                "file3.txt": "Python Backend Development",
                "file4.txt": "Data Processing Engine",
            }

            for name, content in samples.items():
                (self.folder / name).write_text(
                    content,
                    encoding="utf-8",
                )

        for file_path in self.folder.iterdir():
            if file_path.is_file():
                await self.queue.put(file_path)

    async def calculate_hash(self, file_path):
        def read_and_hash():
            sha256 = hashlib.sha256()

            with file_path.open("rb") as file:
                for chunk in iter(
                    lambda: file.read(8192),
                    b"",
                ):
                    sha256.update(chunk)

            return sha256.hexdigest()

        return await asyncio.to_thread(read_and_hash)

    async def process_file(self, file_path):
        if not file_path.exists():
            raise FileNotFoundError(file_path)

        file_hash = await self.calculate_hash(file_path)

        if file_hash in self.seen_hashes:
            self.duplicates += 1

            return {
                "file": file_path.name,
                "status": "DUPLICATE",
                "sha256": file_hash,
            }

        self.seen_hashes.add(file_hash)

        content = await asyncio.to_thread(
            file_path.read_text,
            encoding="utf-8",
        )

        return {
            "file": file_path.name,
            "status": "COMPLETED",
            "size_bytes": file_path.stat().st_size,
            "word_count": len(content.split()),
            "sha256": file_hash,
        }

    async def worker(self, worker_id):
        while True:
            file_path = await self.queue.get()

            try:
                if file_path is None:
                    return

                print(
                    f"[WORKER {worker_id}] "
                    f"Processing {file_path.name}"
                )

                result = await self.process_file(file_path)

                self.results.append(result)

                if result["status"] == "DUPLICATE":
                    print(f"[DUPLICATE] {file_path.name}")
                else:
                    self.processed += 1
                    print(f"[DONE] {file_path.name}")

            except Exception as error:
                self.failed += 1

                self.results.append({
                    "file": file_path.name,
                    "status": "FAILED",
                    "error": str(error),
                })

                print(f"[ERROR] {file_path.name}: {error}")

            finally:
                self.queue.task_done()

    async def run(self):
        await self.setup()

        print(f"Starting {self.workers} workers...\n")

        tasks = [
            asyncio.create_task(self.worker(i + 1))
            for i in range(self.workers)
        ]

        await self.queue.join()

        for _ in tasks:
            await self.queue.put(None)

        await asyncio.gather(*tasks)

        await self.save_report()

    async def save_report(self):
        report = {
            "generated_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "summary": {
                "processed": self.processed,
                "duplicates": self.duplicates,
                "failed": self.failed,
            },
            "files": self.results,
        }

        report_path = self.folder / "report.json"

        await asyncio.to_thread(
            report_path.write_text,
            json.dumps(report, indent=4),
            encoding="utf-8",
        )

        print("\n========== REPORT ==========")
        print(json.dumps(report["summary"], indent=4))
        print(f"\nReport saved to: {report_path}")


async def main():
    processor = AsyncFileProcessor(
        folder="uploads",
        workers=3,
    )

    await processor.run()


if __name__ == "__main__":
    asyncio.run(main())
