import bson
import hashlib
import os
import os.path

import aiofiles
import aiohttp.web as web
# from aiohttp_auth import auth, acl
import aiohttp_cors
import aiohttp_jinja2
import jinja2
from multidict import MultiDict

import skd
import cache_manager

from utils.aio import aio
import utils.authorization as auth
from utils.db_client import db
from utils import json


async def login(request):
    params = await request.json()
    try:
        usr, pwd = params['username'], params['password']
        (user, ) = await db.users.find({'_id': usr}).to_list(None)
    except (KeyError, ValueError):
        return web.HTTPForbidden()
    if hashlib.sha1(user['salt'] + bytes(pwd, encoding='utf-8')).digest() == user['password']:
        await auth.auth.remember(request, usr)
        return web.Response(body='OK'.encode('utf-8'))
    return web.HTTPForbidden()

@auth.acl_required('admin')
async def create_user(request):
    params = await request.json()
    try:
        usr, pwd = params['username'], params['password']
    except KeyError:
        return web.HTTPBadRequest()
    salt = os.urandom(16)
    enc_pwd = hashlib.sha1(salt + bytes(pwd, encoding='utf-8')).digest()
    try:
        await db.users.insert({"_id": usr, "salt": salt, "password": enc_pwd, 'permissions': params.get('permissions', [])})
    except Exception as exc:
        return web.HTTPConflict(text=str(exc))
    else:
        return web.Response(text='user {} created'.format(usr))

@auth.auth.auth_required
async def test_auth_required(request):
    return web.Response(text='authorized')

@auth.acl_required('register_check')
async def test_acl_required(request):
    return web.Response(text='TS check registered')


async def index(request):
    return web.Response(text='SKD rest api')

@auth.acl_required('register_check')
async def receive_task(request):
    print(await request.read())
    task = await request.json()
    groups = await auth.acl.get_user_groups(request)
    try:
        system_code = task['system']
    except KeyError:
        return web.HTTPForbidden(text='System code required!')
    if task['system'] + 'rw' not in groups:
        msg = 'you cannot register task for system {}!'.format(task['system'])
        return web.HTTPForbidden(text=msg)
    return web.json_response(await skd.register_task(task))


def getter(collection):
    @auth.acl_required('view')
    async def route(request):
        try:
            inp = await request.json()
            print(inp, request)
        except json.json.decoder.JSONDecodeError as exc:
            return web.HTTPBadRequest(text=str(exc))
        groups = set(await auth.acl.get_user_groups(request))
        print(await auth.auth.get_auth(request))
        query = json.to_object_id(inp.get('query', {}))

        system = query.get('system')

        required_groups = {'super'}
        msg = 'try filtering by system!'
        if system is not None:
            required_groups.add(system + 'r')
            required_groups.add(system + 'rw')
            msg = 'you cannot view system {}!'.format(system)
        if not groups.intersection(required_groups):
            return web.HTTPForbidden(text=msg)

        sort, skip, limit = inp.get('sort'), inp.get('skip'), inp.get('limit')
        cursor = db.get_collection(collection)
        try:
            cursor = cursor.find(query, inp.get('project'))
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
            return web.json_response(json.dumps(data))
    return route


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
    # async for piece in Request.stream_registry(tdate):
    #     resp.write(piece)
    return resp

async def on_shutdown(app):
    print('server shutting down')
    cache_manager.stop()
    await app['refresher']


def init(loop):
    middlewares = [
        auth.auth_middleware,
        auth.acl_middleware
    ]

    app = web.Application(loop=loop, middlewares=middlewares)
    cors = aiohttp_cors.setup(app, defaults={"*": aiohttp_cors.ResourceOptions(allow_credentials=True)})

    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('./templates'))
    app.router.add_get('/', index)
    cors.add(app.router.add_post('/rest/send_task', receive_task))
    for dimension in 'cache tasks checks'.split():
        cors.add(app.router.add_post('/rest/' + dimension, getter(dimension)))
    # app.router.add_get(trade_system.rest_url, ts)
    # app.router.add_get('/trade_system', trade_system.view)
    # app.router.add_get('/trade_system/send_task', trade_system.send_task)
    app.router.add_get('/files', get_file)

    cors.add(app.router.add_post('/auth', login))
    app.router.add_post('/create_user', create_user)
    cors.add(app.router.add_get('/test_auth_required', test_auth_required))
    app.router.add_get('/test_acl_required', test_acl_required)

    app['refresher'] = aio.ensure_future(cache_manager.refresher())

    app.on_shutdown.append(on_shutdown)
    return app

web.run_app(init(aio.get_event_loop()), port=9000)
