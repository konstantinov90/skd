import copy
import os.path
import re
import yaml

import aiofiles

from utils.environment import *

class CheckExtError(Exception):
    pass


class GitBlobWrapper(object):
    def __init__(self, blob):
        self.full_path, self.hash = blob.abspath, blob.hexsha
        path = blob.abspath
        path, _ext = os.path.splitext(path)
        self.ext = _ext.strip('.').lower()
        check_op_sys = []
        for _ in range(3):
            path, el = os.path.split(path)
            check_op_sys.append(el)
        self.check, self.operation, self.system = check_op_sys

    async def make_check(self):
        if self.ext == 'yml':
            inst = YmlCheck(self)
        elif self.ext == 'sql':
            inst = SqlCheck(self)
        elif self.ext == 'py':
            inst = PyCheck(self)
        else:
            raise CheckExtError(self.full_path, self.ext)
        await inst._init()
        return inst

    @property
    def data(self):
        return {
            'system': self.system,
            'operation': self.operation,
            'name': self.check,
            'extension': self.ext,
            'hash': self.hash
        }


class Check(object):
    def __init__(self, blob):
        self.blob = blob
        self.content, self.meta = {}, {}

    async def _init(self):
        try:
            async with aiofiles.open(self.blob.full_path, 'r', encoding='utf-8') as fd:
                self.content = await fd.read()
        except UnicodeDecodeError:
            async with aiofiles.open(self.blob.full_path, 'r', encoding='windows-1251') as fd:
                self.content = await fd.read()

        self.content = re.sub(';\s*$', '', self.content)

    @property
    def data(self):
        data = copy.deepcopy(self.meta)
        data.update(self.blob.data, content=self.content)
        return data

class YmlCheck(Check):
    async def _init(self):
        await super()._init()
        self.meta = await aio.async_run(yaml.load, self.content)
        self.content = self.meta.pop('query')

        print('yml check {}'.format(self.blob.full_path))


class SqlCheck(Check):
    async def _init(self):
        await super()._init()

        print('sql check {}'.format(self.blob.full_path))


class PyCheck(Check):
    async def _init(self):
        await super()._init()
        check = {'check': "this out"}
        task = {'dummy': 'task'}
        comp = await aio.async_run(compile, self.content, '<string>', 'single')
        exec(comp)
        func = locals()['run_check']
        self.meta = await aio.async_run(yaml.load, func.__doc__) if func.__doc__ else {}

        print('python check {}'.format(self.blob.full_path))
