import time
import asyncio
import httpx


async def run_async(fn, *args, **kwargs):
    loop = asyncio.get_event_loop()
    loop.run_in_executor()
    print("DONE EXECUTING FUNCTION")


async def workflow_executor_wrapper(*args, **kwargs):
    id = kwargs.get("workflow")
    print(f"STARTING EXECUTION {id}")
    await asyncio.sleep(30)
    httpx.get("http://localhost:8000/slow")
    print("DONE DONE DONE")
    # find an executor externally, constructor should take kwargs of the workflow
