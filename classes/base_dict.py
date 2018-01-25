import collections
import copy
import datetime

from utils.db_client import get_db


class BaseDict(collections.UserDict):
    db = get_db()
    collection = 'some_collection'

    def __init__(self, dct):
        dct = {k: v for k, v in dct.items() if k not in ('_id', 'started', 'finished')}
        super().__init__(dct)
        self.oid = None

    @classmethod
    def restore(cls, dct):
        instance = cls(dct)
        instance.oid = dct['_id']
        instance.update(
            _id=dct['_id'],
            started=dct['started'],
        )
        return instance

    def set_db(self, db):
        self.db = db

    @classmethod
    def get_col(cls):
        return cls.db[cls.collection]

    @property
    def col(self):
        return self.get_col()

    @classmethod
    def find(cls, *args):
        return cls.get_col().find(*args)

    @classmethod
    def update_many(cls, *args):
        return cls.get_col().update_many(*args)

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

    # @property
    # def json(self):
    #     dct = copy.deepcopy(self.data)
    #     for s in dct['sources']:
    #         s['password'] = '***'
    #     for k, v in dct.items():
    #         if isinstance(v, (datetime.datetime, bson.ObjectId)):
    #             dct[k] = str(v)
    #         else:
    #             dct[k] = v
    #     return dct
