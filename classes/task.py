from .base_dict import BaseDict


class Task(BaseDict):
    collection = 'tasks'

    async def save(self):
        pwds = [s['password'] for s in self['sources']]
        for s in self['sources']:
            s['password'] = '***'

        await super().save()

        for s, pwd in zip(self['sources'], pwds):
            s['password'] = pwd
