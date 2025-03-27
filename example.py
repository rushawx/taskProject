import random

import asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from taskiq import TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource
from taskiq_redis import RedisStreamBroker, RedisAsyncResultBackend


result_backend = RedisAsyncResultBackend(
    redis_url="redis://localhost:6379",
)

broker = RedisStreamBroker(
    url="redis://localhost:6379",
).with_result_backend(result_backend)

scheduler = TaskiqScheduler(
    broker=broker,
    sources=[LabelScheduleSource(broker)],
)


@broker.task(schedule=[{"cron": "* * * * *"}])
async def my_task() -> str:
    engine = create_async_engine("postgresql+asyncpg://postgres:postgres@localhost:5432/postgres")
    async with engine.begin() as conn:
        random_value = random.randint(0, 100)
        await conn.execute(sa.text(f"""INSERT INTO data ("value") VALUES ({random_value})"""))
    return "Task executed successfully"


async def main() -> None:
    await broker.startup()

    task = await my_task.kiq()

    result = await task.wait_result()

    if not result.is_err:
        print(f"Returned value: {result.return_value}")
    else:
        print("Error found while executing task")
    
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
