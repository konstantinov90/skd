import copy
import datetime
import json

import bson
from aiohttp import web

import settings

_DATETIME_FMT = '%Y-%m-%d %H:%M:%S'

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, bson.ObjectId):
            return str(o)
        elif isinstance(o, datetime.datetime):
            return o.strftime(_DATETIME_FMT)
        return json.JSONEncoder.default(self, o)

if settings.DEBUG:
    _ENCODER = JSONEncoder(indent=2)
else:
    _ENCODER = JSONEncoder()

def dumps(obj, **kwargs):
    return _ENCODER.encode(obj, **kwargs)

def to_object_id(_obj):
    obj = copy.deepcopy(_obj)
    if isinstance(obj, str):
        try:
            return bson.ObjectId(obj)
        except bson.errors.InvalidId:
            try:
                return datetime.datetime.strptime(obj, _DATETIME_FMT)
            except ValueError:
                pass
        return obj
    elif isinstance(obj, dict):
        return {k: to_object_id(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_object_id(v) for v in obj]
    return obj

async def request_parser_middleware(app, handler):
    async def _middleware_handler(request):
        if request.method == 'POST':
            try:
                request['body'] = to_object_id(await request.json())
            except json.JSONDecodeError as exc:
                return web.HTTPBadRequest(text=str(exc))
        else:
            request['body'] = ''
        return await handler(request)

    return _middleware_handler

def response_encoder(handler):
    json_header = {'Content-Type': 'application/json'}
    async def _handler(request):
        resp = await handler(request)
        if isinstance(resp, (dict, list)):
            return web.Response(text=_ENCODER.encode(resp), headers=json_header)
        elif isinstance(resp, web.Response):
            return resp
        else:
            raise ValueError('Unacceptable response!')
    return _handler
