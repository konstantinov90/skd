import os.path
from operator import itemgetter as at


import settings as S
from .base_dict import BaseDict

class Check(BaseDict):
    collection = 'checks'

    async def generate_filename(self, ext):
        out_filename = '{}_{}_{}_{}.{}'.format(
            self['task_id'], *at('system', 'operation', 'name', 'extension')(self)
        )
        if ext:
            out_filename += '.{}'.format(ext)
        out_filename = os.path.join(S.check_result_path, out_filename)
        await self.put(result_filename=out_filename)
