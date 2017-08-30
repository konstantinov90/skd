from classes import Task, Check
from utils.aio import aio
from utils.db_client import db
from utils.environment import py, yml, sql

compare = {
"system": "COMPARE",
"code": "TEST_1",
"sources": [
    {
        "login": "facts",
        "password": "facts",
        "db": "ts_black",
        "ops": {
            "OP": 1
        }
    },
    {
        "login": "facts",
        "password": "facts",
        "db": "ts_black",
        "ops": {
            "OP": 2
        }
    }
]
}


async def register_task(_task):
    task = Task(_task)
    await task.save()
    aio.ensure_future(run_task(task))
    return task.data

async def run_task(task):
    running_checks = []
    query = {
        'system': task['system'],
        'operation': task['code'],
        '$or': task.get('checks', [{}])
    }
    async for _check in db.cache.find(query):
        check = Check(_check)
        if check['extension'] == 'py':
            running_checks.append(aio.ensure_future(py(check, task)))
        elif check['extension'] == 'sql':
            running_checks.append(aio.ensure_future(sql(check, task)))
        elif check['extension'] == 'yml':
            running_checks.append(aio.ensure_future(yml(check, task)))

    await aio.wait(running_checks)
    await task.finish()
