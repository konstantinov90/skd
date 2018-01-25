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
        pwds = [s['password'] for s in self['sources']]
        for s in self['sources']:
            s['password'] = '***'

        if 'key' in self:
            _key = self['key']
            self['key'] = self.key

        await super().save()

        if 'key' in self:
            self['key'] = _key

        for s, pwd in zip(self['sources'], pwds):
            s['password'] = pwd
