import asyncio as aio
import concurrent.futures

import settings as S

try:
    import uvloop
    aio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

policy = aio.get_event_loop_policy()
policy.set_event_loop(policy.new_event_loop())

executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
proc_executor = concurrent.futures.ProcessPoolExecutor()
lock = aio.Semaphore(S.MAX_CONCURRENT_CHECKS)

async def async_run(func, *args):
    return await aio.get_event_loop().run_in_executor(executor, func, *args)

async def proc_run(func, *args):
    return await aio.get_event_loop().run_in_executor(proc_executor, func, *args)

def run(coroutine, *args):
    return aio.get_event_loop().run_until_complete(coroutine(*args))

def loop_run(coroutine, *args):
    return aio.get_event_loop().call_soon(coroutine(*args))