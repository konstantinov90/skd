import collections
from datetime import datetime, timedelta
import hashlib
import weakref

from utils.aio import aio
from utils.db_client import db
from utils import app_log, json_util



LOG = app_log.get_logger()

TEN_SECONDS = timedelta(seconds=10)

PROLONG_RUNNER_SECONDS = 30

def hash_obj(obj):
    dummy = json_util.dumps(obj).encode('utf-8')
    return hashlib.sha1(dummy).hexdigest()

class SingleQueryRunner(object):
    sort_by = (('name', 1), ('extension', 1))

    def __init__(self, query):
        self.query = dict(query)
        self.task = None
        self.run()

    def run(self):
        if self.task and not self.task.done():
            LOG.error('this runner\'s {} task is not done!', self)
        self.task = aio.ensure_future(self.run_query())

    def __call__(self):
        return self.task

    async def run_query(self):
        LOG.info('getting {}', self.query)
        checks_tmpls_query = {'system': self.query['system']}
        if 'operation' in self.query:
            checks_tmpls_query.update(operation=self.query['operation'])

        check_tmpls_map = {
            check_tmpl['key_path']: check_tmpl
            async for check_tmpl in db.cache.find(
                checks_tmpls_query, {'content': 0}
            ).sort(self.sort_by)
        }

        self.query.update(latest=True)
        async for check in db.checks.find(self.query):
            if check['key_path'] in check_tmpls_map:
                check_tmpls_map[check['key_path']].update(check=check)
        response_data = list(check_tmpls_map.values())
        response_hash = hash_obj(response_data)
        return {'data': response_data, 'response_hash': response_hash}


class TTLDictNew(object):
    def __init__(self):
        self.dct = weakref.WeakValueDictionary({})

    def __getitem__(self, key):
        """
        get key representing MongoDB query
        return asyncio.Task instance, executing cached query 
        """
        query, response_hash = key
        return aio.ensure_future(self._await_query(query, response_hash))

    @staticmethod
    async def _prolong_runner(runner):
        LOG.debug('prolonging runner {}', runner)
        await aio.sleep(PROLONG_RUNNER_SECONDS)

    async def _await_query(self, query, response_hash):
        key = json_util.dumps(query)
        runner = self.dct.get(key)
        if not runner:
            runner = SingleQueryRunner(query)
            self.dct[key] = runner
        while response_hash == (await runner())['response_hash']:
            runner.run()
            await aio.sleep(2)

        aio.ensure_future(self._prolong_runner(runner))
        return await runner()





class TTLDict(collections.UserDict):
    def __setitem__(self, _key, _value):
        value = {'value': _value, 'expires': datetime.now() + TEN_SECONDS}
        super().__setitem__(_key, value)

    def __getitem__(self, key):
        return super().__getitem__(key)['value']

    def refresh_item(self, key):
        if key in self:
            self.data[key]['expires'] = datetime.now() + TEN_SECONDS

    def seek_and_destroy(self):
        for k, v in list(self.data.items()):
            if datetime.now() > v['expires']:
                LOG.warning('del {}', v['value'])
                del self[k]
