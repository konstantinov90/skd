import imp
from operator import attrgetter
import datetime
import os
import os.path
import sys
import platform
from subprocess import Popen
import sys

from classes import Task, Check
import settings
from utils import aio, app_log, json_util, environment, db_client
# from utils.db_client import db
from utils.environment import py, yml, sql

LOG = app_log.get_logger()
PROC_NAME = 'python{}'.format('3' if 'linux' in platform.system().lower() else '')
db = db_client.get_db()

def init(path):
    if not os.path.isdir(path):
        os.makedirs(path)

init(settings.CHECK_RESULT_PATH)

async def register_task(_task):
    task = Task(_task)
    await task.save()
    aio.aio.ensure_future(run_task(task))
    return task.data


async def run_check(extension, check, task):
    check.update(
        task_id=task['_id'],
        key=task['key'],
        started=datetime.datetime.now()
    )
    cached_code = check.pop('content')
    await check.save()
    result = await aio.proc_run(run_check_process, check['extension'], check, task, cached_code)
    await check.finish(result=result)
    print(check.filename)
    if os.path.isfile(check.filename):
        await check.put(result_filename=check.filename)
        await check.calc_crc32()

def run_check_process(extension, check, task, cached_code):
    # imp.reload(aio)
    # imp.reload(db_client)
    # sys.modules.clear()
    # imp.reload(environment)
    
    return aio.loop_run(attrgetter(extension)(environment), check, task, cached_code)

# def run_sql(check, task):
#     aio.run(environment.sql, check, task)

# def run_yml(check, task):
#     aio.run(environment.yml, check, task)

# def run_py(check, task):
#     aio.run(environment.py, check, task)

async def run_task(task):
    running_checks = []
    query = {
        'system': task['system'],
        'operation': task['code'],
        '$or': task.get('checks', [{}])
    }
    LOG.info('query {}', query)
    async for _check in db.cache.find(query):
        check = Check(_check)
        running_checks.append(aio.aio.ensure_future(run_check(check['extension'], check, task)))
        # if check['extension'] == 'py':
        #     running_checks.append(aio.aio.ensure_future(py(check, task)))
        #     # running_checks.append(aio.aio.ensure_future(aio.proc_run(aio.run(py, check, task))))
        # elif check['extension'] == 'sql':
        #     running_checks.append(aio.aio.ensure_future(sql(check, task)))
        #     # running_checks.append(aio.aio.ensure_future(aio.proc_run(run_check, check['extension'], check, task)))
        # elif check['extension'] == 'yml':
        #     running_checks.append(aio.aio.ensure_future(yml(check, task)))
            # running_checks.append(aio.aio.ensure_future(aio.proc_run(aio.run(yml, check, task))))
        # proc = Popen([PROC_NAME, '{}.py'.format(__name__), json_util.dumps(_check), json_util.dumps(task.data)], start_new_session=True)
        # running_checks.append(aio.aio.ensure_future(aio.async_run(proc.wait)))

    await aio.aio.wait(running_checks)
    await task.finish()

if __name__ == '__main__':
    # try:
    #     import resource
    # except ModuleNotFoundError:
    #     pass
    # else:
    #     half_gig = 2**29
    #     resource.setrlimit(resource.RLIMIT_AS, (half_gig, half_gig))
    _check = json_util.to_object_id(json_util.json.loads(sys.argv[1])) 
    check = Check(_check)
    task = json_util.to_object_id(json_util.json.loads(sys.argv[2]))
    if check['extension'] == 'py':
        aio.run(py, check, task)
    elif check['extension'] == 'sql':
        aio.run(sql, check, task)
    elif check['extension'] == 'yml':
        aio.run(yml, check, task)
    