import copy
import datetime

import pymongo

from utils.environment import py, sql

rio = {
"system": "COMPARE",
"code": "TEST_2",
"sources": [
    {
        "login": "konstantinov",
        "password": "Kuh4i2m",
        "db": "ts_eur_1",
        "ops": {
            "TSID": 221926701
        }
    },
    {
        "login": "konstantinov",
        "password": "Kuh4i2m",
        "db": "ts_eur_1",
        "ops": {
            "TSID": 221924701
        }
    }
]

}

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

source = {
"system": "TSIII",
"code": "RDDISTR",
"uid": "fb48b4ed-d117-4e27-80ee-ba3579f2e73e",
"user": "konstantinov",
"sources": [
    {
        "login": "facts",
        "password": "facts",
        "db": "ts_black",
        "ops": {
            "CONSCALC": 1442,
            "LOADPOWDEM": 56601,
            "CALENDAR": 56401,
            "LOADPREVDR": 62801,
            "LOADMXFACT": 63101,
            "LOADSD": 62601,
            "AUXCOEFF": 63401,
            "RDDISTR": 1409,
            "LOADBIDS": 62301,
            "LOADRD": 63201,
            "GENLIMITS": 1410,
            "LOADCRM": 62001,
            "LOADMODEL": 63501
        }
    }
],
}

cli = pymongo.MongoClient('vm-ts-blk-app2')
db = cli.skd_cache
tasks, cache = db.tasks, db.cache

def run_task(_task):
    task = copy.deepcopy(_task)
    pwds = [s['password'] for s in task['sources']]
    for s in task['sources']:
        s['password'] = '***'
    task.update(started=datetime.datetime.now())
    oid = tasks.insert(task)
    for s, pwd in zip(task['sources'], pwds):
        s['password'] = pwd

    for check in cache.find({'system': task['system'], 'operation': task['code']}, {'_id': 0}):
        if check['extension'] == 'py':
            py(check, task)
        elif check['extension'] == 'yml':
            sql(check, task)

    tasks.update_one({'_id': oid}, {'$set': {'finished': datetime.datetime.now()}})
