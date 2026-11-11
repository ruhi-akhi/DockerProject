
import asyncio
import uuid
from contextlib import asynccontextmanager


class FakeDatabaseConnection:
    def __init__(self):
        self.connection_id = str(uuid.uuid4())[:8]
        self.connected = True

    async def execute(self, query):
        if not self.connected:
            raise RuntimeError("Connection is closed")

        await asyncio.sleep(0.5)

        return {
            "connection_id": self.connection_id,
            "query": query,
            "status": "success",
        }

    async def close(self):
        self.connected = False


class AsyncConnectionPool:
    def __init__(self, max_connections=3):
        self.max_connections = max_connections

        self.available = asyncio.Queue()
        self.all_connections = []
        self.semaphore = asyncio.Semaphore(max_connections)

        self.created = 0
        self.active = 0
        self.closed = False

    async def initialize(self):
        for _ in range(self.max_connections):
            connection = FakeDatabaseConnection()

            self.all_connections.append(connection)
            await self.available.put(connection)

            self.created += 1

        print(
            f"Pool initialized with "
            f"{self.created} connections"
        )

    @asynccontextmanager
    async def acquire(self):
        if self.closed:
            raise RuntimeError("Connection pool is closed")

        await self.semaphore.acquire()

        connection = None

        try:
            connection = await self.available.get()
            self.active += 1

            print(
                f"[ACQUIRED] {connection.connection_id} "
                f"| Active: {self.active}"
            )

            yield connection

        finally:
            if connection is not None:
                self.active -= 1

                if connection.connected and not self.closed:
                    await self.available.put(connection)

                    print(
                        f"[RELEASED] {connection.connection_id} "
                        f"| Active: {self.active}"
                    )

            self.semaphore.release()

    async def close(self):
        self.closed = True

        for connection in self.all_connections:
            await connection.close()

        print("\nAll database connections closed.")

    def stats(self):
        return {
            "total_connections": self.created,
            "active_connections": self.active,
            "available_connections": self.available.qsize(),
        }


async def database_request(pool, request_id):
    print(f"\nRequest {request_id} started")

    async with pool.acquire() as connection:
        result = await connection.execute(
            f"SELECT * FROM users WHERE id = {request_id}"
        )

        print(f"[RESULT] Request {request_id}: {result}")


async def main():
    pool = AsyncConnectionPool(max_connections=3)

    await pool.initialize()

    print("\nStarting 8 concurrent database requests...\n")

    await asyncio.gather(
        *[
            database_request(pool, i)
            for i in range(1, 9)
        ]
    )

    print("\n========== POOL STATISTICS ==========")

    print(pool.stats())

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
