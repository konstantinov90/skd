import base64
import hashlib
from operator import itemgetter
import os
import os.path
import re

import aiofiles
import aiohttp.web as web
# from aiohttp_auth import auth, acl
import aiohttp_cors
import aiohttp_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from cryptography import fernet
from multidict import MultiDict

import skd
import cache_manager

import settings
from utils.aio import aio
import utils.authorization as auth
from utils.db_client import db
from utils import json_util
from utils import app_log


async def index(request):
    return web.Response(text='SKD rest api')

async def login(request):
    params = request['body']
    try:
        usr, pwd = params.pop('username'), params.pop('password')
        (user, ) = await db.users.find({'_id': usr}).to_list(None)
    except KeyError:
        return web.HTTPBadRequest()
    except ValueError:
        return web.HTTPForbidden()

    # session = await aiohttp_session.get_session(request)
    # session['_id'] = fernet.Fernet.generate_key().decode()
    # print(session['_id'], request.headers['user-agent'])
    # session.update(params)

    if hashlib.sha1(user['salt'] + bytes(pwd, encoding='utf-8')).digest() == user['password']:
        await auth.auth.remember(request, usr)
        return web.Response(body='OK'.encode('utf-8'))
    return web.HTTPForbidden()

@auth.acl_required('admin')
async def create_user(request):
    try:
        usr, pwd = request['body']['username'], request['body']['password']
    except KeyError:
        return web.HTTPBadRequest()
    salt = os.urandom(16)
    enc_pwd = hashlib.sha1(salt + bytes(pwd, encoding='utf-8')).digest()
    try:
        user = {"_id": usr, "salt": salt, "password": enc_pwd, "permissions": request['body'].get('permissions', [])}
        await db.users.insert(user)
    except Exception as exc:
        return web.HTTPConflict(text=str(exc))
    else:
        return web.Response(text='user {} created'.format(usr))

@auth.system_required('register_check')
@json_util.response_encoder
async def receive_task(request):
    return await skd.register_task(request['body'])

mem_cache = {}

async def get_last_checks(query):
    regexpr = re.compile('^(?!_).*')

    checks = {}
    key = itemgetter(*'system operation name extension'.split())
    print(query)
    async for task in db.tasks.find(query).sort([('started', -1)]):
        task_id = task['_id']
        async for check in db.checks.find({'task_id': task_id}).sort([('name', 1), ('extension', 1)]):
            current_key = key(check)
            if current_key in checks:
                continue
            checks[current_key] = check
    check_tmpls = await db.cache.find({
        'system': query['system'],
        'operation': regexpr
    }, {'content': 0}).sort([('operation', 1), ('name', 1), ('extension', 1)]).to_list(None)
    for check_tmpl in check_tmpls:
        check = checks.get(key(check_tmpl))
        if check:
            check_tmpl['check'] = check
    return check_tmpls


@auth.system_required('view')
@json_util.response_encoder
async def cached_get_last_checks(request):

    def hash_obj(obj):
        dummy = bytes(json_util.dumps(obj), encoding='utf-8')
        return hashlib.sha1(dummy).digest()

    query = request['body']['query']
    force_reload = request['body'].get('force_reload')
    key = hash_obj(query)
    # print(request['key'])

    check_tmpls = None

    check_tmpls = await get_last_checks(query)
    if force_reload:
        print(request.headers)
        mem_cache[key] = check_tmpls
        return check_tmpls

    while check_tmpls == mem_cache[key]:
        check_tmpls = await get_last_checks(query)
        await aio.sleep(1)
        print('tick', query)

    mem_cache[key] = check_tmpls
    return check_tmpls


def getter(collection):

    @auth.system_required('view')
    @json_util.response_encoder
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
    task_id = request['body']['task_id']
    msg = ''
    async for check in db.checks.find({'task_id': task_id}):
        filesize = os.stat(check['result_filename']).st_size
        msg += '{} {} {}\n'.format(filesize, check['result_filename'], check['name'])
    return web.Response(text=msg, headers={"X-Archive-Files": "zip"})


async def get_file(request):
    query = request.query
    _, filename = os.path.split(query['f'])
    cnt_dsp = 'attachment; filename="{}"'.format(filename)
    ext = filename.split('.')[-1]
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
    resp.content_length = os.stat(query['f']).st_size
    await resp.prepare(request)
    async with aiofiles.open(query['f'], 'rb') as fd:
        resp.write(await fd.read())
    return resp

async def on_shutdown(app):
    print('server shutting down')
    cache_manager.stop()
    await app['refresher']

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

    app.router.add_get('/', index)
    cors.add(app.router.add_post('/rest/send_task', receive_task))
    for dimension in 'cache tasks checks'.split():
        cors.add(app.router.add_post('/rest/' + dimension, getter(dimension)))
    app.router.add_post('/rest/get_last_checks', cached_get_last_checks)
    app.router.add_get('/files', get_file)
    app.router.add_post('/archive', get_archive)

    cors.add(app.router.add_post('/auth', login))
    app.router.add_post('/create_user', create_user)

    app['refresher'] = aio.ensure_future(cache_manager.refresher())

    app.on_shutdown.append(on_shutdown)
    return app

web.run_app(init(aio.get_event_loop()), port=settings.PORT)
