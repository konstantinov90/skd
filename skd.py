import os
import os.path
import sys
import pickle
import platform
from subprocess import Popen

from classes import Task, Check
import settings
from utils import aio, app_log, json_util
from utils.db_client import db
from utils.environment import py, yml, sql

LOG = app_log.get_logger()
PROC_NAME = 'python{}'.format('3' if 'linux' in platform.system().lower() else '')

def init(path):
    if not os.path.isdir(path):
        os.makedirs(path)

init(settings.CHECK_RESULT_PATH)

async def register_task(_task):
    task = Task(_task)
    await task.save()
    aio.aio.ensure_future(run_task(task))
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
            running_checks.append(aio.aio.ensure_future(py(check, task)))
        elif check['extension'] == 'sql':
            running_checks.append(aio.aio.ensure_future(sql(check, task)))
        elif check['extension'] == 'yml':
            running_checks.append(aio.aio.ensure_future(yml(check, task)))
        # proc = Popen([PROC_NAME, '{}.py'.format(__name__), json_util.dumps(_check), json_util.dumps(task.data)])
        # running_checks.append(aio.aio.ensure_future(aio.async_run(proc.wait)))

    await aio.aio.wait(running_checks)
    await task.finish()

if __name__ == '__main__':
    try:
        import resource
    except ModuleNotFoundError:
        pass
    else:
        half_gig = 2**29
        resource.setrlimit(resource.RLIMIT_AS, (half_gig, half_gig))

    _check = json_util.to_object_id(json_util.json.loads(sys.argv[1])) 
    check = Check(_check)
    task = json_util.to_object_id(json_util.json.loads(sys.argv[2]))
    # task = Task(_task)
    if check['extension'] == 'py':
        aio.run(py, check, task)
    elif check['extension'] == 'sql':
        aio.run(sql, check, task)
    elif check['extension'] == 'yml':
        aio.run(yml, check, task)
    