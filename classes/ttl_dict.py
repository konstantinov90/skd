import collections
from datetime import datetime, timedelta
from utils.aio import aio
from utils import app_log

LOG = app_log.get_logger()

TEN_SECONDS = timedelta(seconds=10)

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
