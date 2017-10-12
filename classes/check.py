import os.path
from operator import itemgetter as at
import zlib
import aiofiles

import settings
from .base_dict import BaseDict

_BASE16 = 2**32

class Check(BaseDict):
    collection = 'checks'

    async def save(self):
        self['latest'] = True
        await self.col.update_one({
            'system': self['system'],
            'operation': self['operation'],
            'name': self['name'],
            'extension': self['extension'],
            'key': self['key'],
            'latest': True,
        }, {'$set': {'latest': False}})
        await super().save()

    @property
    def rel_filename(self):
        return '{}_{}_{}_{}.{}.{}'.format(
            self['task_id'],
            *at('system', 'operation', 'name', 'extension')(self),
            self.get('result_extension', 'xlsx'),
        )

    @property
    def filename(self):
        try:
            _filename = self.rel_filename
        except KeyError:
            return
        return os.path.join(settings.CHECK_RESULT_PATH, _filename)

    # async def generate_filename(self):
    #     out_filename = '{}_{}_{}_{}.{}'.format(
    #         self['task_id'], *at('system', 'operation', 'name', 'extension')(self)
    #     )
    #     # out_filename = os.path.join(S.check_result_path, out_filename)
    #     await self.put(result_filename=out_filename)

    async def calc_crc32(self):
        async with aiofiles.open(self.filename, 'rb') as fd:
            crc = zlib.crc32(await fd.read()) % _BASE16
        await self.put(result_crc32=format(crc, 'x'))
