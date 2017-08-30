import os.path
from operator import itemgetter as at
import zlib
import aiofiles

import settings
from .base_dict import BaseDict

_BASE16 = 2**32

class Check(BaseDict):
    collection = 'checks'

    @property
    def filename(self):
        try:
            return os.path.join(settings.CHECK_RESULT_PATH, self['result_filename'])
        except KeyError:
            return

    async def generate_filename(self, ext):
        out_filename = '{}_{}_{}_{}.{}'.format(
            self['task_id'], *at('system', 'operation', 'name', 'extension')(self)
        )
        if ext:
            out_filename += '.{}'.format(ext)
        # out_filename = os.path.join(S.check_result_path, out_filename)
        await self.put(result_filename=out_filename)

    async def calc_crc32(self):
        async with aiofiles.open(self.filename, 'rb') as fd:
            crc = zlib.crc32(await fd.read()) % _BASE16
        await self.put(result_crc32=format(crc, 'x'))
