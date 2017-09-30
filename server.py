#! /usr/bin/env python3
import base64
import bson
import hashlib
from operator import itemgetter
import os
import os.path
import sys
import tracemalloc
import urllib.parse
import weakref

import aiofiles
import aiohttp.web as web
import aiohttp_cors
# import aiohttp_session
import pymongo.errors
# from aiohttp_session.cookie_storage import EncryptedCookieStorage
# from cryptography import fernet
from multidict import MultiDict

import skd
import cache_manager
from classes.ttl_dict import TTLDictNew
import settings
from utils import aio, app_log, authorization as auth, json_util
from utils.db_client import db

LOG = app_log.get_logger()
MEM_LOG = app_log.get_logger('memory')

async def index(request):
    return 'SKD rest api'

async def login(request):
    try:
        usr, pwd = request['body']['username'], request['body']['password']
        (user, ) = await db.users.find({'_id': usr}).to_list(None)
    except KeyError:
        return web.HTTPBadRequest()
    except ValueError:
        return web.HTTPForbidden()

    if hashlib.sha1(user['salt'] + pwd.encode('utf-8')).digest() == user['password']:
        await auth.auth.remember(request, usr)
        return 'OK'
    return web.HTTPForbidden()

@auth.acl_required('admin')
async def create_user(request):
    try:
        usr, pwd = request['body']['username'], request['body']['password']
    except KeyError:
        return web.HTTPBadRequest()
    salt = os.urandom(16)
    permissions = request['body'].get('permissions', [])
    enc_pwd = hashlib.sha1(salt + pwd.encode('utf-8')).digest()
    try:
        user = {"_id": usr, "salt": salt, "password": enc_pwd, "permissions": permissions}
        await db.users.insert(user)
    except pymongo.errors.PyMongoError as exc:
        return web.HTTPConflict(text=str(exc))
    else:
        return 'user {} created'

# @auth.system_required('register_check')
async def receive_task(request):
    return await skd.register_task(request['body'])

# @auth.system_required('view')

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


def getter(collection):
    # @auth.system_required('view')
    async def route(request):
        query = request['body']['query']

        sort = request['body'].get('sort')
        skip = request['body'].get('skip')
        limit = request['body'].get('limit')

        cursor = db.get_collection(collection)
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
        'Content-Type': content_type
    }))
    filepath = os.path.join(settings.CHECK_RESULT_PATH, filename)
    resp.content_length = os.stat(filepath).st_size
    await resp.prepare(request)
    async with aiofiles.open(filepath, 'rb') as fd:
        resp.write(await fd.read())
    return resp

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
        await aio.aio.sleep(0.5)

async def on_shutdown(app):
    LOG.info('server shutting down')
    app['running'] = False

    cancel_task = aio.aio.ensure_future(aio.aio.gather(
        app['mem_log'],
        # app['result_cache'],
        app['refresher'],
    ))

    for req in app['mem_cache_requests']:
        if not req.done():
            req.cancel()

    await cancel_task


def init(loop):
    tracemalloc.start()
    # cache_manager.Cache()

    middlewares = [
        json_util.request_parser_middleware,
        auth.auth_middleware,
        auth.acl_middleware,
        app_log.access_log_middleware,
    ]

    app = web.Application(loop=loop, middlewares=middlewares)

    cors = aiohttp_cors.setup(app, defaults={"*": aiohttp_cors.ResourceOptions(allow_credentials=True)})

    # fernet_key = fernet.Fernet.generate_key()
    # secret_key = base64.urlsafe_b64decode(fernet_key)
    # aiohttp_session.setup(app, EncryptedCookieStorage(secret_key))

    app.middlewares.append(json_util.response_encoder_middleware)

    cors.add(app.router.add_get('/', index))
    cors.add(app.router.add_post('/rest/send_task', receive_task))
    for dimension in 'cache tasks checks'.split():
        cors.add(app.router.add_post('/rest/' + dimension, getter(dimension)))
    cors.add(app.router.add_post('/rest/get_last_checks', cached_get_last_checks))
    app.router.add_get('/files/{filename}', get_file)
    app.router.add_post('/archive', get_archive)
    app.router.add_get('/archive', get_archive)

    cors.add(app.router.add_post('/auth', login))
    app.router.add_post('/create_user', create_user)

    app['running'] = True
    app['mem_cache'] = TTLDictNew()
    app['mem_cache_requests'] = weakref.WeakSet()
    app['refresher'] = aio.aio.ensure_future(cache_manager.Cache().refresher(app))
    # app['result_cache'] = aio.aio.ensure_future(get_last_checks(app))
    app['mem_log'] = aio.aio.ensure_future(memory_log(app))
    app.on_shutdown.append(on_shutdown)
    return app

if __name__ == '__main__':
    web.run_app(init(aio.aio.get_event_loop()), port=settings.PORT)

