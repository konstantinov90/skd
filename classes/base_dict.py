import bson
import collections
import copy
import datetime

from utils.db_client import db

class BaseDict(collections.UserDict):
    db = db
    collection = 'some_collection'

    def __init__(self, dct):
        super().__init__(dct)
        self.oid = None

    @property
    def col(self):
        return self.db[self.collection]

    async def save(self):
        self['started'] = datetime.datetime.now()
        self.oid = await self.col.insert(self.data)

    async def put(self, **kwargs):
        if not self.oid:
            raise ValueError('cannot put to unsaved {}'.format(self))
        self.update(**kwargs)
        await self.col.update_one({'_id': self.oid}, {'$set': kwargs})

    # async def start(self):
    #     await self.put(started=datetime.datetime.now())

    async def finish(self, **kwargs):
        await self.put(finished=datetime.datetime.now(), **kwargs)

    @property
    def json(self):
        dct = copy.deepcopy(self.data)
        for s in dct['sources']:
            s['password'] = '***'
        for k, v in dct.items():
            if isinstance(v, (datetime.datetime, bson.ObjectId)):
                dct[k] = str(v)
            else:
                dct[k] = v
        return dct
