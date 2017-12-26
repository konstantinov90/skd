import os
import os.path
import platform
import sys
import traceback
from operator import attrgetter

from aiohttp import web

import settings
from classes import Check, Task
from utils import aio, app_log, db_client, environment, json_util

LOG = None
db = db_client.get_db()

def init(path):
    if not os.path.isdir(path):
        os.makedirs(path)

init(settings.CHECK_RESULT_PATH)

async def run_check(check, task, cached_code, env_logger):
    try: 
        result = await attrgetter(check['extension'])(environment)(check, task, cached_code, env_logger)
        await check.finish(result=result)
        if os.path.isfile(check.filename):
            await check.put(result_filename=check.rel_filename)
            await check.calc_crc32()
    except Exception as e:
        LOG.error(r'\n'.join(traceback.format_tb(e.__traceback__)) + str(e))
    finally:
        await check.put(running=False)

async def index(request):
    return web.Response(text=request.app['port'])

async def entry(request):
    data = json_util.to_object_id(await request.json())
    _check, task, cached_code = data['check'], data['task'], data['cached_code']
    check = Check.restore(_check)
    await check.put(running=True)
    aio.aio.ensure_future(run_check(check, task, cached_code, request.app['env_logger']))

    return web.Response(text="ok")


def start(port_str):
    app = web.Application()
    app.router.add_get('/', index)
    app.router.add_post('/entry/', entry)
    app['port'] = port_str
    app['env_logger'] = app_log.get_logger(f'env_{port_str}')
    web.run_app(app, loop=aio.aio.get_event_loop(), port=int(port_str))


if __name__ == '__main__':
    port_str = sys.argv[1]
    LOG = app_log.get_logger(f'worker_{port_str}')    
    start(port_str)
