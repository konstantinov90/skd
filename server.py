#! /usr/bin/env python3
import base64
import datetime
import hashlib
import os
import os.path
import platform
import random
import sys
import traceback
import tracemalloc
import urllib.parse
import weakref
from operator import itemgetter
from subprocess import Popen

import aiofiles
import aiohttp
import aiohttp_cors
import bson
import pymongo.errors
from aiohttp import web
from multidict import MultiDict

import cache_manager
import settings
import skd
from classes import Check, Task, Cache
from classes.ttl_dict import TTLDictNew
from utils import authorization as auth
from utils import aio, app_log, json_util
# from utils.db_client import get_db
# hello master
# hello rebase

LOG = app_log.get_logger()
MEM_LOG = app_log.get_logger('memory')
# db = get_db()

async def index(request):
    return 'SKD rest api'

async def receive_task(request):
    # return await skd.register_task(request['body'])

    task = Task(request['body'])
    await task.save()

    query = {
        'system': task['system'],
        'operation': task['operation'],
        '$or': task.get('checks', [{}])
    }
    async for _check in Cache.find(query):
        check = Check(_check)
        check.task = task
        await request.app['queue'].put(check)
    return task.data

def remember_task(handler):
    async def _handler(request):
        task = aio.aio.ensure_future(handler(request))
        request.app['mem_cache_requests'].add(task)
        return await task
    return _handler

@remember_task
async def cached_get_last_checks(request):
    mem_cache = request.app['mem_cache']

    query = request['body']['query']
    response_hash = request['body'].get('response_hash')
    return await mem_cache[query, response_hash]


def getter(dimension):
    # @auth.system_required('view')
    async def route(request):
        query = request['body']['query']

        sort = request['body'].get('sort')
        skip = request['body'].get('skip')
        limit = request['body'].get('limit')

        cursor = dimension.get_col()
        try:
            cursor = cursor.find(query, request['body'].get('project'))
            if sort is not None:
                cursor = cursor.sort(sort)
            if skip is not None:
                cursor = cursor.skip(skip)
            if limit is not None:
                cursor = cursor.limit(limit)
            data = await cursor.to_list(None)
        except Exception as exc:
            return web.HTTPBadRequest(text='{}\nRemember to use pymongo syntax!\n'.format(str(exc)))
        else:
            return data
    return route
#{'system': 'NSS', 'key': {'target_date': datetime.datetime(2017, 1, 29, 0, 0), 'tsid': 221562601, 'database': 'ts_azure'}, 'operation': 'COMMON', 'latest': True}
#{'system': 'NSS', 'key': {'target_date': datetime.datetime(2017, 1, 29, 0, 0), 'tsid': 221562601, 'database': 'ts_azure'}, 'latest': True, 'operation': 'COMMON'}

async def get_archive(request):
    if request['body']:
        task_id = request['body']['task_id']
    else:
        task_id = bson.ObjectId(request.query['task_id'])
    msg = ''
    (task,) = await db.tasks.find({'_id': task_id}).to_list(None)
    task['operation'] = task['code']
    del task['_id']
    del task['code']
    del task['sources']
    del task['started']
    del task['finished']
    del task['checks']
    key = hash_obj(task)
    LOG.info('archive task {} {}', hash_obj(task), json_util.dumps(request.app['mem_cache'][hash_obj(task)]['response']))

    for check_tmpl in request.app['mem_cache'][key]['response']:
        check = check_tmpl.get('check')
        if not check:
            continue
        filepath = os.path.join(settings.CHECK_RESULT_PATH, check['result_filename'])
        fake_filepath = os.path.join('skd/files', check['result_filename'])
        enc_path = urllib.parse.quote(fake_filepath.encode('utf-8'))
        filesize = os.stat(filepath).st_size
        _, ext = os.path.splitext(check['result_filename'])
        out_filename = check['name'] + ext
        msg += '{} {} /{} {}\n'.format(check['result_crc32'], filesize, enc_path, out_filename)
    return web.Response(text=msg, headers={"X-Archive-Files": "zip", "Content-Disposition": "attachment; filename=archive.zip"})


async def get_file(request):
    print('getting file')
    filename = request.match_info.get('filename')
    enc_filename = urllib.parse.quote(filename.encode('utf-8'))
    cnt_dsp = 'attachment; filename="{}"'.format(filename)
    ext = os.path.splitext(filename)[-1]
    if ext == 'xlsx':
        content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif ext == 'xls':
        content_type = 'application/vnd.ms-excel'
    else:
        content_type = "application/octet-stream"

    resp = web.StreamResponse(headers=MultiDict({
        'CONTENT-DISPOSITION': cnt_dsp,
        'Content-Type': content_type,
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': '*',
    }))
    filepath = os.path.join(settings.CHECK_RESULT_PATH, filename)
    resp.content_length = os.stat(filepath).st_size
    await resp.prepare(request)
    async with aiofiles.open(filepath, 'rb') as fd:
        await resp.write(await fd.read())
    return resp

async def queue_size(request):
    return web.Response(text=str(request.app['queue'].qsize()))

# from logging import LogRecord
# import sys
# import gc

async def memory_log(app):
    while app['running']:
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        lr = []
        # for obj in gc.get_objects():
        #     if isinstance(obj, LogRecord):
        #         lr.append(obj)
        MEM_LOG.info('='*40)
        MEM_LOG.info('mem cache keys {}', len(list(app['mem_cache'].dct.keys())))
        for stat in top_stats[:10]:
            MEM_LOG.info('{}', stat)
        await aio.aio.sleep(10)

async def on_shutdown(app):
    LOG.info('server shutting down')
    app['running'] = False

    cancel_task = aio.aio.ensure_future(aio.aio.gather(
        app['mem_log'],
        # app['result_cache'],
        app['refresher'],
    ))

    app['queue_processing'].cancel()

    for req in app['mem_cache_requests']:
        if not req.done():
            req.cancel()

    for proc in app['workers'].values():
        proc.terminate()
        print(f'process {proc} terminated')

    await cancel_task

async def start_workers(app):
    worker_path = os.path.join(os.path.dirname(__file__), 'skd.py')

    app['workers'] = {}
    proc_name = 'python{}'.format('3' if 'linux' in platform.system().lower() else '')
    for port in settings.WORKER_PORTS:
        app['workers'][port] = Popen([proc_name, worker_path, str(port)])

async def clear_running_checks(app):
    await Check.update_many({'running': True}, {'$set': {'running': False}})

async def process_checks(app):
    while app['running']:
        check = await app['queue'].get()

        aio.aio.ensure_future(send_check(check, app))

async def get_idle_worker(session, app):
    reqs = []
    for port in app['workers']:
        url = f'http://localhost:{port}'
        reqs.append(session.get(url))

    done, pending = await aio.aio.wait(reqs, timeout=5)
    for future in pending:
        future.cancel()

    if not done:
        LOG.info('error')
        await aio.aio.sleep(10)
        return await get_idle_worker(session, app)
    (resp,) = random.sample(done, 1)
    LOG.info(str(resp))
    try:
        port = await resp.result().text()
        LOG.info('found worker at {}', port)
    except Exception as e:
        LOG.error(r'\n'.join(traceback.format_tb(e.__traceback__)) + str(e))
    return port


async def send_check(check, app):
    try:
        async with aiohttp.ClientSession(json_serialize=json_util.dumps) as session:
            task = check.task
            # cached_code = check.pop('content')
            # await check.save()
            check.update(
                task_id=task['_id'],
                key=task.key,
                started=datetime.datetime.now(),
            )
            cached_code = check.pop('content')

            await check.save()

            port = await get_idle_worker(session, app)
            url = f'http://localhost:{port}/entry/'
            
            try:
                async with session.post(url, data=json_util.dumps({'task': task.data, 'check': check.data, 'cached_code': cached_code})) as resp:
                    assert (await resp.text()) == "ok"
                await check.put(submitted_to=url)
            except Exception as e:
                LOG.error(r'\n'.join(traceback.format_tb(e.__traceback__)) + str(e))
    except Exception as e:
        LOG.error(r'\n'.join(traceback.format_tb(e.__traceback__)) + str(e))
    # await check.finish(result=result)
    # if os.path.isfile(check.filename):
    #     await check.put(result_filename=check.rel_filename)
    #     await check.calc_crc32()



def init(loop):
    tracemalloc.start()
    # cache_manager.Cache()

    middlewares = [
        web.normalize_path_middleware(redirect_class=web.HTTPTemporaryRedirect),
        json_util.request_parser_middleware,
        auth.auth_middleware,
        auth.acl_middleware,
        app_log.access_log_middleware,
    ]

    app = web.Application(loop=loop, middlewares=middlewares)

    cors = aiohttp_cors.setup(app, defaults={"*": aiohttp_cors.ResourceOptions(expose_headers='*', allow_headers='*', allow_credentials=True)})

    # fernet_key = fernet.Fernet.generate_key()
    # secret_key = base64.urlsafe_b64decode(fernet_key)
    # aiohttp_session.setup(app, EncryptedCookieStorage(secret_key))

    app.middlewares.append(json_util.response_encoder_middleware)

    cors.add(app.router.add_get('/', index))
    cors.add(app.router.add_post('/rest/send_task/', receive_task))
    for route, dimension in zip(('cache', 'tasks', 'checks'), (Cache, Task, Check)):
        cors.add(app.router.add_post(f'/rest/{route}/', getter(dimension)))
    cors.add(app.router.add_post('/rest/get_last_checks/', cached_get_last_checks))
    cors.add(app.router.add_get('/files/{filename}', get_file))
    app.router.add_post('/archive/', get_archive)
    app.router.add_get('/archive/', get_archive)
    app.router.add_get('/queue_size/', queue_size)

    app['running'] = True
    app['mem_cache'] = TTLDictNew()
    app['mem_cache_requests'] = weakref.WeakSet()
    app['refresher'] = aio.aio.ensure_future(cache_manager.Cache().refresher(app))
    app['queue'] = aio.aio.Queue(loop=loop)
    app['queue_processing'] = aio.aio.ensure_future(process_checks(app))
    # app['result_cache'] = aio.aio.ensure_future(get_last_checks(app))
    app['mem_log'] = aio.aio.ensure_future(memory_log(app))
    app.on_startup.append(clear_running_checks)
    app.on_startup.append(start_workers)
    app.on_shutdown.append(on_shutdown)
    return app

if __name__ == '__main__':
    web.run_app(init(aio.aio.get_event_loop()), port=settings.PORT)
