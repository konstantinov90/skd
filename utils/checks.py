import copy
import functools
import os.path
import yaml
from .environment import *

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

    def make_check(self):
        if self.ext == 'yml':
            inst = SqlCheck(self)
        elif self.ext == 'py':
            inst = PyCheck(self)
        else:
            raise CheckExtError(self.full_path)
        return inst

    def get_data(self):
        return {
            'system': self.system,
            'operation': self.operation,
            'check': self.check,
            'extension': self.ext,
            'hash': self.hash
        }


class Check(object):
    def __init__(self, blob):
        self.blob = blob
        with open(self.blob.full_path, 'r', encoding='utf-8') as f:
            self.content = f.read()

    def get_data(self):
        data = self.blob.get_data()
        data.update(content=self.content)
        return data


class SqlCheck(Check):
    def __init__(self, blob):
        super().__init__(blob)

        self.meta = yaml.load(self.content)
        self.content = self.meta['query']
        del self.meta['query']

        print('sql check {}'.format(self.blob.full_path))

    def get_data(self):
        data = copy.deepcopy(self.meta)
        data.update(super().get_data())
        return data


class PyCheck(Check):
    def __init__(self, blob):
        super().__init__(blob)
        check = {'check': "this out"}
        task = {'dummy': 'task'}
        exec(compile(self.content, '<string>', 'single'))
        func = locals()['run_check']
        self.meta = yaml.load(func.__doc__) if func.__doc__ else {}

        print('python check {}'.format(self.blob.full_path))

    def get_data(self):
        data = copy.deepcopy(self.meta)
        data.update(super().get_data())
        return data
