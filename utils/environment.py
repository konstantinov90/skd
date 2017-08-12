import copy
import csv
import datetime
import functools
import os.path
from collections import abc
from operator import itemgetter as at

import pymongo

from . import DB
from .zip_join import zip_join

cli = pymongo.MongoClient('vm-ts-blk-app2')
db = cli.skd_cache
checks = db.checks

def single_connection(check, task):
    def decorator(target_func):

        @functools.wraps(target_func)
        def result_func(*args):
            (con_data,) = task['sources']
            con = DB.OracleConnection(*at('login', 'password', 'db')(con_data))

            sources = con_data['ops']

            return target_func(*args, con, sources)
        return result_func
    return decorator

def double_connection(check, task):
    def decorator(target_func):

        @functools.wraps(target_func)
        def result_func(*args):
            if len(task['sources']) != 2:
                raise Exception('wrong number of connections for check {}'.format(check['_id']))
            fwd = []
            for con_data in task['sources']:
                fwd.append(DB.OracleConnection(*at('login', 'password', 'db')(con_data)))
                fwd.append(con_data['ops'])
            return target_func(*args, *fwd)
        return result_func
    return decorator


def output_file_descriptor(check, task):
    def decorator(target_func):

        @functools.wraps(target_func)
        def result_func(*args):
            with open(check['result_filename'], 'w') as fd:
                return target_func(*args, fd)

        return result_func
    return decorator


def environment(target):
    def decorated_func(check, _task):
        task = copy.deepcopy(_task)

        check['task_id'] = task['_id']
        check['started'] = datetime.datetime.now()
        cached_code = check['content']
        del check['content']
        oid = checks.insert(check)

        out_filename = '{}_{}_{}_{}.{}.csv'.format(
            oid, *at('system', 'operation', 'check', 'extension')(check)
        )
        out_filename = os.path.join('check_results', out_filename)
        checks.update_one({'_id': oid}, {'$set': {'result_filename': out_filename}})
        check['result_filename'] = out_filename

        try:
            result = target(cached_code, check, task)
            if isinstance(result, abc.Sequence): # deconstruct complex result
                if isinstance(result[0], bool) or result[0] is None:
                    logical_result, result = result
                elif isinstance(result[-1], bool) or result[-1] is None:
                    result, logical_result = result
                else:
                    logical_result = None
                # then save the fuck out of it!
                if os.path.isfile(check['result_filename']):
                    raise Exception('cannot save file twice!')
                with open(check['result_filename'], 'w') as fd:
                    writer = csv.writer(fd, delimiter=';', lineterminator='\n',
                                        quoting=csv.QUOTE_MINIMAL)
                    writer.writerows(result)
            else:
                logical_result = result
        except Exception as exc:
            print(exc)
            logical_result = 'runtime error: {}'.format(exc)

        # update на время, когда выполнилась проверка
        checks.update_one({'_id': oid}, {'$set': {
            'finished': datetime.datetime.now(),
            'result': logical_result
        }})

    return decorated_func

@environment
def py(cached_code, check, task):
    try:
        eval(compile(cached_code, '<string>', 'single'))
        print(locals()['run_check'].__doc__)
        return locals()['run_check']()
    except Exception as exc:
        raise exc

@environment
def sql(query, check, task):
    if check['type'] == 'LOGICAL':

        @single_connection(check, task)
        def run_check(con, source):
            res = con.exec_script(query, get_field_names=True, **source)
            return len(res) > 1, res
    elif check['type'] == 'INFO':
        @single_connection(check, task)
        def run_check(con, source):
            return con.exec_script(query, get_field_names=True, **source)
    elif check['type'] == 'COMPARE':
        @double_connection(check, task)
        def run_check(con_a, source_a, con_b, source_b):
            a_head, b_head = None, None
            a_new, a_old, b_new, b_old = [], [], [], []
            for row in con_a.script_cursor(query, get_field_names=True, **source_a):
                if not a_head:
                    a_head = row
                    continue
                a_new.append(('a',) + row)
                a_old.append(('b',) + row)
            for row in con_b.script_cursor(query, get_field_names=True, **source_b):
                if not b_head:
                    b_head = row
                    continue
                b_new.append(('b',) + row)
                b_old.append(('a',) + row)
            for row in b_old:
                if row in a_new:
                    a_new.remove(row)
            for row in a_old:
                if row in b_new:
                    b_new.remove(row)
            res_list = a_new + b_new
            res_list = sorted(res_list, key=at(1, 2, 0))
            return [('порядок',) + a_head] + res_list

    return run_check()
