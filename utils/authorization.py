from functools import partial
import hashlib
import os

from aiohttp import web
import aiohttp_auth.auth as auth
import aiohttp_auth.acl as acl

from .db_client import db
from . import aio

auth_policy = auth.CookieTktAuthentication(os.urandom(32), 6000000, include_ip=True)
auth_middleware = auth.auth_middleware(auth_policy)

async def acl_group_callback(usr):
    try:
        (user,) = await db.users.find({'_id': usr}).to_list(None)
    except ValueError:
        return ()
    else:
        return tuple(user['permissions'])

acl_middleware = acl.acl_middleware(acl_group_callback)

async def get_acl_context():
    context = []
    async for row in db.acl.find():
        context.append((
            row['permission'], row['group'], tuple(row['actions'])
        ))
    return context

def acl_required(permission):
    return acl.decorators.acl_required(permission, aio.run(get_acl_context))

def system_required(permission):
    print('system required factory')
    def _decorator(target_func):
        print('system required decorator')
        @acl_required(permission)
        async def _wrapper(request):
            groups = set(await acl.get_user_groups(request))

            system = request['body'].get('query', {}).get('system')

            required_groups = {'super'}
            msg = 'try filtering by system!'
            if system is not None:
                required_groups.add(system + 'r')
                required_groups.add(system + 'rw')
                msg = 'you cannot view system {}!'.format(system)
            if not groups.intersection(required_groups):
                return web.HTTPForbidden(text=msg)
            return await target_func(request)

        return _wrapper
    return _decorator
