import asyncio

async def coro():
    loop = asyncio.get_event_loop()
    print('hello from coro! id =', id(loop))

def func():
    asyncio.new_event_loop().run_until_complete(coro())
