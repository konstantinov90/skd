import copy
import os.path
import re
import yaml

import aiofiles

from utils import app_log
from utils.environment import *

LOG = app_log.get_logger()

class CheckExtError(Exception):
    pass


class GitBlobWrapper(object):
    def __init__(self, system, blob):
        self.system = system
        check_op = []
        path = blob.abspath
        for _ in range(2):
            path, el = os.path.split(path)
            check_op.append(el.strip('"'))

        self.filename, self.operation = check_op
        self.check, self.ext = os.path.splitext(self.filename)
        self.ext = self.ext.strip('.').lower()
        self.full_path = os.path.join(path, self.operation, self.filename)
        self.hash = blob.hexsha
        self.key_path = os.path.join(self.system, self.operation, self.filename)

    def __repr__(self):
        return self.full_path

    async def make_check(self):
        if not os.path.isfile(self.full_path):
            raise CheckExtError('ignoring directory {}'.format(self.full_path))

        if self.ext == 'yml':
            inst = YmlCheck(self)
        elif self.ext == 'sql':
            inst = SqlCheck(self)
        elif self.ext == 'py':
            inst = PyCheck(self)
        else:
            raise CheckExtError(self.full_path, self.ext)
        try:
            await inst._init()
        except UnicodeDecodeError:
            raise
        return inst

    @property
    def data(self):
        return {
            'system': self.system,
            'operation': self.operation,
            'name': self.check,
            'extension': self.ext,
            'hash': self.hash,
            'key_path': self.key_path,
        }


class Check(object):
    def __init__(self, blob):
        self.blob = blob
        self.content, self.meta = {}, {}

    async def _init(self):
        async def read_file(filename, encoding='utf-8'):
            async with aiofiles.open(filename, 'r', encoding=encoding) as fd:
                return await fd.read()
        for enc in 'utf-8-sig', 'utf-8', 'windows-1251':
            try:
                self.content = await read_file(self.blob.full_path, encoding=enc)
            except UnicodeDecodeError:
                continue
            else:
                break
        else:
            raise UnicodeDecodeError
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

        # LOG.info('yml check {}', self.blob.full_path)


class SqlCheck(Check):
    async def _init(self):
        await super()._init()

        # LOG.info('sql check {}', self.blob.full_path)


class PyCheck(Check):
    async def _init(self):
        await super()._init()
        check = {'check': "this out"}
        task = {'dummy': 'task'}
        try:
            comp = await aio.async_run(compile, self.content, '<string>', 'single')
            exec(comp)
        except Exception as e:
            LOG.error('error compiling func')
            print(e)
        else:
            func = locals()['run_check']
            self.meta = await aio.async_run(yaml.load, func.__doc__) if func.__doc__ else {}
            LOG.info('%s', self.meta)
            if isinstance(self.meta, str):
                self.meta = {'meta': self.meta}

        # LOG.info('python check {}', self.blob.full_path)
