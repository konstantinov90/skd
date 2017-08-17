import os.path
from operator import itemgetter

import aiofiles
import aiohttp.web as web
import aiohttp_jinja2
import jinja2
import motor.motor_asyncio as motor
from multidict import MultiDict

import skd
import cache_manager
from view_controller import trade_system
from utils.aio import aio
from utils.db_client import db

# db = motor.AsyncIOMotorClient('vm-ts-blk-app2').skd_cache
# checks = db.checks

app = web.Application()
aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('./templates'))


async def index(request):
    return web.Response(text='SKD rest api')

app.router.add_get('/', index)

async def receive_task(request):
    task = await request.json()
    # try:
    #     await skd.run_task(task)
    # except Exception as exc:
    #     return web.Response(text='{}'.format(exc))
    # aio.ensure_future(skd.run_task(task))
    return web.json_response(await skd.register_task(task))

app.router.add_post('/rest/send_task', receive_task)

# async def get_checks(request):
#     query = await request.json()
#     resp = await checks.find_one(query)
#     resp['_id'] = str(resp['_id'])
#     resp['task_id'] = str(resp['task_id'])
#     resp['started'] = str(resp['started'])
#     resp['finished'] = str(resp['finished'])
#     return web.json_response(resp)
#
# app.router.add_post('/rest/get_checks', get_checks)

async def ts(request):
    return web.json_response(await trade_system.controller(request))

app.router.add_get(trade_system.rest_url, ts)
app.router.add_get('/trade_system', trade_system.view)
app.router.add_get('/trade_system/send_task', trade_system.send_task)

async def get_file(request):
    query = request.query
    _, filename = path, el = os.path.split(query['f'])
    cnt_dsp = 'attachment; filename="{}"'.format(filename)

    resp = web.StreamResponse(headers=MultiDict({
        'CONTENT-DISPOSITION': cnt_dsp
    }))
    resp.content_type = 'text/xml'
    # resp.content_length = len(registry)
    await resp.prepare(request)
    async with aiofiles.open(query['f'], 'rb') as fd:
        resp.write(await fd.read())
    # async for piece in Request.stream_registry(tdate):
    #     resp.write(piece)
    return resp

app.router.add_get('/files', get_file)

app['refresher'] = aio.ensure_future(cache_manager.refresher())
async def on_shutdown(app):
    print('server shutting down')
    cache_manager.stop()
    await app['refresher']

app.on_shutdown.append(on_shutdown)

web.run_app(app, port=9000)
