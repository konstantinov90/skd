import copy
import os.path
import re
import yaml

import aiofiles

from utils import app_log
from utils.environment import *

LOG = app_log.get_logger(__name__)

class CheckExtError(Exception):
    pass


class GitBlobWrapper(object):
    def __init__(self, blob):
        check_op = []
        for _ in range(2):
            path, el = os.path.split(blob.abspath)
            print(path)
            check_op.append(el.strip('"'))

        self.filename, self.operation = check_op
        LOG.error(self.filename)
        LOG.warning(self.operation)
        self.check, self.ext = os.path.splitext(self.filename)
        self.ext = self.ext.strip('.').lower()
        LOG.error(self.check, '->>', self.ext)
        self.full_path = os.path.join(path, self.operation, self.filename)
        LOG.warning(self.full_path)

        self.hash = blob.hexsha
        _, self.system = os.path.split(blob.repo.working_tree_dir)

    def __repr__(self):
        return self.full_path

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
        async def read_file(filename, encoding='utf-8'):
            async with aiofiles.open(self.blob.full_path, 'r', encoding=encoding) as fd:
                return await fd.read()
        try:
            self.content = await read_file(self.blob.full_path)
        except UnicodeDecodeError:
            self.content = await read_file(self.blob.full_path, encoding='windows-1251')

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

        LOG.info('yml check {}', self.blob.full_path)


class SqlCheck(Check):
    async def _init(self):
        await super()._init()

        LOG.info('sql check {}', self.blob.full_path)


class PyCheck(Check):
    async def _init(self):
        await super()._init()
        check = {'check': "this out"}
        task = {'dummy': 'task'}
        comp = await aio.async_run(compile, self.content, '<string>', 'single')
        exec(comp)
        func = locals()['run_check']
        self.meta = await aio.async_run(yaml.load, func.__doc__) if func.__doc__ else {}

        LOG.info('python check {}', self.blob.full_path)
