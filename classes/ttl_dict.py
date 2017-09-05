import collections
from datetime import datetime, timedelta
from utils.aio import aio

TEN_SECONDS = timedelta(seconds=10)

class TTLDict(collections.UserDict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.running = True

    def __setitem__(self, _key, _value):
        value = {'value': _value, 'expires': datetime.now() + TEN_SECONDS}
        super().__setitem__(_key, value)

    def __getitem__(self, key):
        return super().__getitem__(key)['value']

    def refresh_item(self, key):
        if key in self:
            self[key]['expires'] = datetime.now() + TEN_SECONDS

    def seek_and_destroy(self):
        for k, v in list(self.data.items()):
            if datetime.now() > v['expires']:
                print("DELETING KEY " + k)
                del self[k]

    async def activate(self):
        while self.running:
            sleep_task = aio.ensure_future(aio.sleep(2))
            self.seek_and_destroy()
            await sleep_task

    def stop(self):
        self.running = False
