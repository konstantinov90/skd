import base64
import bson
import hashlib
from operator import itemgetter
import os
import os.path
import tracemalloc
import urllib.parse

import aiofiles
import aiohttp.web as web
import aiohttp_cors
import aiohttp_session
import pymongo.errors
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from cryptography import fernet
from multidict import MultiDict

import skd
import cache_manager
from classes.ttl_dict import TTLDict
import settings
from utils import app_log, authorization as auth, json_util
from utils.aio import aio
from utils.db_client import db

LOG = app_log.get_logger()
MEM_LOG = app_log.get_logger('mem_log', r'logs/memory.log')

tracemalloc.start()

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

_KEY = itemgetter(*'system operation name extension'.split())

# async def _get_last_checks(query):
#     checks = {}
#     print(query)
#     async for task in db.tasks.find(query).sort([('started', -1)]):
#         task_id = task['_id']
#         async for check in db.checks.find({'task_id': task_id}).sort([('name', 1), ('extension', 1)]):
#             current_key = _KEY(check)
#             if current_key in checks:
#                 continue
#             checks[current_key] = check
#     check_tmpls = await db.cache.find({
#         'system': query['system'],
#         # 'operation': _REGEXPR,
#     }, {'content': 0}).sort([('operation', 1), ('name', 1), ('extension', 1)]).to_list(None)
#     for check_tmpl in check_tmpls:
#         check = checks.get(_KEY(check_tmpl))
#         if check:
#             check_tmpl['check'] = check
#     return check_tmpls

def hash_obj(obj):
    dummy = json_util.dumps(obj).encode('utf-8')
    return hashlib.sha1(dummy).hexdigest()

async def get_last_checks_portion(key, query):
    LOG.debug('{} getting check {}', key, query)

    checks_tmpls_query = {'system': query['system']}
    sort_by = (('name', 1), ('extension', 1))
    if 'operation' in query:
        checks_tmpls_query.update(operation=query['operation'])

    check_tmpls_map = {}
    async for check_tmpl in db.cache.find(checks_tmpls_query, {'content': 0}).sort(sort_by):
        current_key = _KEY(check_tmpl)
        check_tmpls_map[current_key] = check_tmpl

    query['latest'] = True
    async for check in db.checks.find(query):
        current_key = _KEY(check)
        check_tmpls_map[current_key].update(check=check)
    response_data = list(check_tmpls_map.values())
    mem_cache[key].update(response=response_data, hash=hash_obj(response_data))


async def get_last_checks():
    while True:
        tasks = [aio.sleep(0.5)]
        for k, v in list(mem_cache.items()):
            tasks.append(aio.ensure_future(get_last_checks_portion(k, v['query'])))
        await aio.wait(tasks)
        mem_cache.seek_and_destroy()


# @auth.system_required('view')
async def cached_get_last_checks(request):

    query = request['body']['query']
    response_hash = request['body'].get('response_hash')
    key = hash_obj(query)
    # print(request['key'])
    mem_cache.setdefault(key, {'query': query, 'hash': response_hash})

    while response_hash == mem_cache[key]['hash']:
        mem_cache.refresh_item(key)
        await aio.sleep(0.1)

    return {'data': mem_cache[key]['response'], 'response_hash': mem_cache[key]['hash']}


def getter(collection):
    @auth.system_required('view')
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

async def get_archive(request):
    if request['body']:
        task_id = request['body']['task_id']
    else:
        task_id = bson.ObjectId(request.query['task_id'])
    msg = ''
    async for check in db.checks.find({'task_id': task_id}):
        if 'finished' not in check or 'result_filename' not in check:
            continue
        filepath = os.path.join(settings.CHECK_RESULT_PATH, check['result_filename'])
        enc_path = urllib.parse.quote(filepath.encode('utf-8'))
        filesize = os.stat(filepath).st_size
        _, ext = os.path.splitext(check['result_filename'])
        out_filename = check['name'] + ext
        msg += '{} {} /{} {}\n'.format(check['result_crc32'], filesize, enc_path, out_filename)
    return web.Response(text=msg, headers={"X-Archive-Files": "zip"})


async def get_file(request):
    print('getting file')
    filename = request.match_info.get('filename')
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

from logging import LogRecord
import sys
import gc

async def memory_log():
    while True:
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        MEM_LOG.info('='*40)
        lr = []
        for obj in gc.get_objects():
            if isinstance(obj, LogRecord):
                lr.append(obj)
        MEM_LOG.info('='*40)
        MEM_LOG.info('log records {}', len(lr))
        for stat in top_stats[:20]:
            MEM_LOG.info('{}', stat)
        await aio.sleep(10)

async def on_shutdown(app):
    print('server shutting down')
    cache_manager.stop()
    await app['refresher']

mem_cache = TTLDict()

def init(loop):
    middlewares = [
        json_util.request_parser_middleware,
        auth.auth_middleware,
        auth.acl_middleware,
        app_log.access_log_middleware,
    ]

    app = web.Application(loop=loop, middlewares=middlewares)

    cors = aiohttp_cors.setup(app, defaults={"*": aiohttp_cors.ResourceOptions(allow_credentials=True)})

    fernet_key = fernet.Fernet.generate_key()
    secret_key = base64.urlsafe_b64decode(fernet_key)
    aiohttp_session.setup(app, EncryptedCookieStorage(secret_key))

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

    app['refresher'] = aio.ensure_future(cache_manager.refresher())
    # app['mem_cache'] = aio.ensure_future(mem_cache.activate())
    aio.ensure_future(get_last_checks())
    aio.ensure_future(memory_log())
    app.on_shutdown.append(on_shutdown)
    return app

web.run_app(init(aio.get_event_loop()), port=settings.PORT)
