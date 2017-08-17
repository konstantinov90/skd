import bson
import json
from operator import itemgetter

import aiohttp_jinja2

from utils.aio import aio
from utils.db_client import db
import settings

ts_fields = '_id operation name type started finished result rotten file'.split()
# at_ts_fields = itemgetter(*ts_fields)
key = itemgetter(*'operation name extension'.split())
rest_url = '/rest/ts'


@aiohttp_jinja2.template('system.html')
async def view(request):
    link = '..{}?trade_session_id={}'.format(rest_url, request.query.get('trade_session_id', ''))
    return {'cols': ts_fields, 'link': link, 'settings': settings}

@aiohttp_jinja2.template('send_task.html')
async def send_task(request):
    check_id = bson.ObjectId(request.query['check_id'])
    (check,) = await db.checks.find({'_id': check_id}).to_list(None)
    (task,) = await db.tasks.find(
        {'_id': check['task_id']},
        {'_id': 0, 'started': 0, 'finished': 0, 'checks': 0}
    ).to_list(None)
    task['checks'] = [{'name': check['name'], 'extension': check['extension']}]
    return {'task': json.dumps(task, ensure_ascii=False, indent=4), 'settings': settings}

async def controller(request):
    try:
        tsid = int(request.query['trade_session_id'])
    except (ValueError, KeyError):
        tsid_future = aio.ensure_future(db.tasks.aggregate([
            {'$match': {'system': 'TS'}},
            {'$group': {"_id": 1, 'tsid': {'$max': '$trade_session_id'}}}
        ]).to_list(None))
        (tsid,) = await tsid_future
        tsid = tsid.pop('tsid')

    check_tmpls_future = aio.ensure_future(db.cache.find(
        {'system': 'TS'},
        {'content': 0, 'system': 0})
        .sort((('operation', 1), ('check', 1))
    ).to_list(None))

    checks_future = aio.ensure_future(db.checks.aggregate([
        {'$match': {'system': 'TS'}},
        {'$lookup': {
            'from': 'tasks',
            'localField': 'task_id',
            'foreignField': '_id',
            'as': 'task'
        }},
        {'$unwind': '$task'},
        {'$match': {'task.trade_session_id': tsid}}
    ]).to_list(None))

    # check_tmpls, checks = await aio.gather(check_tmpls_future, checks_future)


    checks = {key(c): c for c in await checks_future}

    data = []
    for check_tmpl in await check_tmpls_future:
        check = checks.get(key(check_tmpl), None)
        if check:
            _id = str(check['_id'])
            started = str(check.get('started', ''))
            finished = str(check.get('finished', ''))
            if 'result' in check:
                if check['result'] is None:
                    result = settings.IMAGES.INFO
                elif not check['result']:
                    result = settings.IMAGES.FAIL
                else:
                    result = settings.IMAGES.PASS
            else:
                result = ''
            if check['hash'] == check_tmpl['hash']:
                rotten = settings.IMAGES.FRESH
            else:
                rotten = settings.IMAGES.ROTTEN
            if 'result_filename' in check and 'result' in check:
                href = '/files?f={}'.format(check['result_filename'])
                _file = '<a href="{}">{}</a>'.format(href, settings.IMAGES.file(check['result_filename']))
            else:
                _file = ''
            href = '/trade_system/send_task?check_id={}'.format(_id)
            name = '<a href="{}">{}</a>'.format(href, check['name'])
        else:
            _id = str(check_tmpl['_id'])
            started = ''
            finished = ''
            result = ''
            rotten = ''
            _file = ''
            name = check_tmpl['name']
        operation = check_tmpl['operation']
        check_type = check_tmpl.get('type', check_tmpl['extension'])
        data.append((_id, operation, name, check_type, started, finished, result, rotten, _file))

    return {'data': data}

# async def controller(request):
#     query = {'system': 'TS'}
#     if 'trade_session_id' in request.query:
#         try:
#             query['trade_session_id'] = int(request.query['trade_session_id'])
#         except ValueError:
#             pass
#     checks_curs = aio.ensure_future(db.tasks.aggregate([
#         {'$match': query},
#         {'$group': {'_id': '$code', 'time': {'$max': '$started'}}},
#         {'$lookup': {
#             'from': 'tasks',
#             'localField': 'time',
#             'foreignField': 'started',
#             'as': 'task'
#         }},
#         {'$unwind': '$task'},
#         {'$lookup': {
#             'from': 'checks',
#             'localField': 'task._id',
#             'foreignField': 'task_id',
#             'as': 'check'
#         }},
#         {'$unwind': '$check'},
#         {'$project': {'_id': '$check._id', 'operation': '$check.operation', 'name': '$check.name',
#             'extension': '$check.extension', 'hash': '$check.hash', 'type': '$check.type',
#             'started': '$check.started', 'filename': '$check.result_filename',
#             'finished': '$check.finished', 'result': '$check.result'}}
#     ]).to_list(None))
#     tmpls = {}
#     async for tmpl in db.cache.find({'system': 'TS'}):
#         tmpls[key(tmpl)] = tmpl['hash']
#
#     checks = await checks_curs
#     for check in checks:
#         if key(check) not in tmpls or tmpls[key(check)] != check['hash']:
#             check['rotten'] = '''<img width="50px" src="{}/images/rotten_apple.png"/>'''.format(settings.STATIC_URL)
#         else:
#             check['rotten'] = '''<img width="50px" src="{}/images/green_apple.png"/>'''.format(settings.STATIC_URL)
#         check['_id'] = str(check['_id'])
#         if 'filename' in check:
#             href = '/files?f={}'.format(check['filename'])
#             check['filename'] = '<a href="{}">{}</a>'.format(href, check['filename'])
#         else:
#             check['filename'] = ''
#         check['started'] = str(check.get('started', ''))
#         check['finished'] = str(check.get('finished', ''))
#         if 'result' in check:
#             if check['result'] is None:
#                 check['result'] = '''<img width="50px" src="{}/images/Emblem-important-yellow.svg.png"/>'''.format(settings.STATIC_URL)
#             elif not check['result']:
#                 check['result'] = '''<img width="50px" src="{}/images/x.png"/>'''.format(settings.STATIC_URL)
#             else:
#                 check['result'] = '''<img width="45px" src="{}/images/checkmark-xxl.png"/>'''.format(settings.STATIC_URL)
#         else:
#             check['result'] = ''
#         check['type'] = check.get('type', check['extension'])
#
#     return {'data': [at_ts_fields(check) for check in checks]}
