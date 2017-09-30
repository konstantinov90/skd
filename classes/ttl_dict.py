import collections
from datetime import datetime, timedelta
import hashlib
import weakref

from utils.aio import aio
from utils.db_client import db
from utils import app_log, json_util

LOG = app_log.get_logger()

# TEN_SECONDS = timedelta(seconds=10)

PROLONG_RUNNER_SECONDS = 10

def hash_obj(obj):
    dummy = json_util.dumps(obj).encode('utf-8')
    return hashlib.sha1(dummy).hexdigest()

class SingleQueryRunner(object):
    sort_by = (('name', 1), ('extension', 1))
    project = {'content': 0}

    def __init__(self, query):
        self.checks_tmpls_query = {'system': query['system']}
        if 'operation' in query:
            self.checks_tmpls_query.update(operation=query['operation'])

        self.query = dict(query)
        self.query.update(latest=True)

        self.task = None
        self.response = {}
        self.subscribers = weakref.WeakSet()
        self.running = None

    def __del__(self):
        LOG.debug('{} is destroyed', self)
        self.running = False

    def subscribe(self, sub):
        """add subscriber to the weak set"""
        self.subscribers.add(sub)
        if len(self.subscribers) == 1:
            self._run()

    async def release(self):
        await aio.sleep(PROLONG_RUNNER_SECONDS)
        if not self.subscribers:
            self.running = False

    def _run(self):
        if self.task and not self.task.done():
            return
        self.task = aio.ensure_future(self._run_query())

    async def _run_query(self):
        self.running = True
        while self.running:
            # LOG.info('getting {}', self.query)

            check_tmpls_map = {
                check_tmpl['key_path']: check_tmpl
                async for check_tmpl in db.cache.find(
                    self.checks_tmpls_query, self.project
                ).sort(self.sort_by)
            }

            async for check in db.checks.find(self.query):
                if check['key_path'] in check_tmpls_map:
                    check_tmpls_map[check['key_path']].update(check=check)
            response_data = list(check_tmpls_map.values())
            response_hash = hash_obj(response_data)
            self.response = {'data': response_data, 'response_hash': response_hash}

            if not self.subscribers:
                # aio.ensure_future(self.release())
                self.running = False
                continue
            await aio.sleep(0.2)

class T():
    pass

class TTLDictNew(object):
    def __init__(self):
        self.dct = weakref.WeakValueDictionary({})

    def __getitem__(self, item_key):
        """
        get key representing MongoDB query
        return asyncio.Task instance, executing cached query 
        """
        query, response_hash = item_key
        return aio.ensure_future(self._await_query(query, response_hash))

    async def _await_query(self, query, response_hash):
        key = json_util.dumps(query)
        runner = self.dct.get(key)
        if not runner:
            runner = SingleQueryRunner(query)
            self.dct[key] = runner
        sub = T()
        runner.subscribe(sub)
        while response_hash == runner.response.get('response_hash', response_hash):
            await aio.sleep(0.1)

        return runner.response


# class TTLDict(collections.UserDict):
#     def __setitem__(self, _key, _value):
#         value = {'value': _value, 'expires': datetime.now() + TEN_SECONDS}
#         super().__setitem__(_key, value)

#     def __getitem__(self, key):
#         return super().__getitem__(key)['value']

#     def refresh_item(self, key):
#         if key in self:
#             self.data[key]['expires'] = datetime.now() + TEN_SECONDS

#     def seek_and_destroy(self):
#         for k, v in list(self.data.items()):
#             if datetime.now() > v['expires']:
#                 LOG.warning('del {}', v['value'])
#                 del self[k]
