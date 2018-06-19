import re
from operator import itemgetter

from bson.son import SON

from .base_dict import BaseDict

SORTING_FN = itemgetter(0)


class Task(BaseDict):
    collection = 'tasks'

    @property
    def key(self):
        return SON(sorted(self.get('key',{}).items(), key=SORTING_FN))

    async def save(self):
        if sel
        con_strs = [s['connection_string'] for s in self['sources']]
        for s in self['sources']:
            if s['class_name'] == 'OracleConnection':
                s['connection_string'] = re.sub(r'(?<=\/)([^@]*)(?=@)', '***', s['connection_string'])
            elif s['class_name'] == 'PostgresConnection':
                s['connection_string'] = re.sub(r'(?<=password)(\s*=\s*)([^\s]*)', r'\1***', s['connection_string'])

        if 'key' in self:
            _key = self['key']
            self['key'] = self.key

        await super().save()

        if 'key' in self:
            self['key'] = _key

        for s, con_str in zip(self['sources'], con_strs):
            s['connection_string'] = con_str
