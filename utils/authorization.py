from functools import partial
import hashlib
import os

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
