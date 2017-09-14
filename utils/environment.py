import csv
import datetime
import functools
import inspect
import traceback
from collections import abc
from operator import itemgetter
import os
import re

import aiofiles
import xlsxwriter

from utils import DB, aio
from utils.aiofiles_adapter import Adapter
from utils.zip_join import zip_join
from utils import app_log

LOG = app_log.get_logger('env')
get_ora_con_str = itemgetter('login', 'password', 'db')

def single_connection(check, task):
    def decorator(target_func):
        @functools.wraps(target_func)
        async def result_func(*args):
            (con_data,) = task['sources']
            if inspect.iscoroutinefunction(target_func):
                con = await DB.OracleConnection.get(*get_ora_con_str(con_data))
                return await target_func(*args, con, con_data['ops'])
            else:
                con = DB.OracleConnection(*get_ora_con_str(con_data))
                return target_func(*args, con, con_data['ops'])
        return result_func
    return decorator

def double_connection(check, task):
    if len(task['sources']) != 2:
        raise ValueError('wrong number of connections for check {}'.format(check['_id']))

    def decorator(target_func):
        if inspect.iscoroutinefunction(target_func):
            @functools.wraps(target_func)
            async def result_func(*args):
                fwd = []
                for con_data in task['sources']:
                    con = await DB.OracleConnection.get(*get_ora_con_str(con_data))
                    fwd += con, con_data['ops']
                return await target_func(*args, *fwd)
        else:
            @functools.wraps(target_func)
            async def result_func(*args):
                fwd = []
                for con_data in task['sources']:
                    con = DB.OracleConnection(*get_ora_con_str(con_data))
                    fwd += con, con_data['ops']
                return target_func(*args, *fwd)
        return result_func
    return decorator


def output_file_descriptor(check, task, ext=None):
    def decorator(target_func):
        # filename = lambda: check['result_filename'] + ('.' + ext if ext else '')

        @functools.wraps(target_func)
        async def result_func(*args):
            await check.generate_filename(ext)
            if inspect.iscoroutinefunction(target_func):
                async with aiofiles.open(check.filename, 'w') as fd:
                    res = await target_func(*args, fd)
            else:
                fd = await aio.async_run(open, check.result_filename, 'w')
                res = await aio.async_run(target_func, *args, fd)
                fd.close()
            await check.calc_crc32()
            return res

        return result_func
    return decorator


def environment(target):
    async def decorated_func(check, task):
        # task = copy.deepcopy(_task)
        await aio.lock.acquire()

        check.update(
            task_id=task['_id'],
            key=task['key'],
            started=datetime.datetime.now()
        )
        cached_code = check.pop('content')
        await check.save()

        try:
            result = await target(cached_code, check, task)

            if isinstance(result, abc.Sequence) and not isinstance(result, str):
                # deconstruct complex result
                if result and isinstance(result[0], bool) or result[0] is None:
                    logical_result, result = result
                elif result and isinstance(result[-1], bool) or result[-1] is None:
                    result, logical_result = result
                else:
                    logical_result = None
                # then save the fuck out of it!
                if 'result_filename' in check:
                    raise Exception('cannot save file twice!')


                if len(result) <= 10000:
                    await check.generate_filename('xlsx')
                    wb = xlsxwriter.Workbook(check.filename)
                    sh = wb.add_worksheet(check['name'][:31])
                    for i, row in enumerate(result):
                        # for j, el in enumerate(row):
                        await aio.async_run(sh.write_row, i, 0, row)
                    await aio.async_run(wb.close)
                    await check.calc_crc32()

                else:
                    await check.generate_filename('csv')
                    adapter = Adapter()
                    writer = csv.writer(adapter, delimiter=';', lineterminator='\n',
                                        quoting=csv.QUOTE_MINIMAL)
                    writer.writerows(result)
                    
                    async with aiofiles.open(check.filename, 'w') as fd:
                        await fd.write(adapter.lines)

            else:
                logical_result = result
        except Exception as exc:
            traceback.print_exc()
            LOG.error('check: {}.{} failed with error: {}', check['name'], check['extension'], exc)
            logical_result = 'runtime error: {}'.format(exc)

        # update на время, когда выполнилась проверка
        await check.finish(result=logical_result)
        aio.lock.release()

    return decorated_func

def print(msg, check_id):
    LOG.info('check {} said: {}', check_id, msg)

@environment
async def py(cached_code, check, task):

    try:
        logging_cached_code = re.sub('print((.*))', r'print(\1, check_id="{}")'.format(check['_id']), cached_code)
        # logging_cached_code = cached_code
        eval(compile(logging_cached_code, '<string>', 'single'))

        func = locals()['run_check']
        ans = await func()
        return ans
    except Exception as exc:
        raise exc

async def sql(check, task):
    check['type'] = 'LOGICAL'
    await yml(check, task)

@environment
async def yml(query, check, task):
    if check['type'] == 'LOGICAL':

        @single_connection(check, task)
        async def run_check(con, source):
            res = await con.async_exec_script(query, get_field_names=True, **source)
            return len(res) > 1, res

    elif check['type'] == 'INFO':

        @single_connection(check, task)
        async def run_check(con, source):
            return await con.async_exec_script(query, get_field_names=True, **source)

    elif check['type'] == 'COMPARE':

        @double_connection(check, task)
        async def run_check(con_a, source_a, con_b, source_b):

            async def process_query(con, query, source, get_field_names):
                res, head = [], None
                cursor = con.async_script_cursor(query, get_field_names, **source)
                if get_field_names:
                    head = await cursor.__anext__()
                async for row in cursor:
                    res.append(row)
                if get_field_names:
                    return head, res
                return res

            proc_a = aio.aio.ensure_future(process_query(con_a, query, source_a, True))
            proc_b = aio.aio.ensure_future(process_query(con_b, query, source_b, False))
            (head, a_new), b_new = await aio.aio.gather(proc_a, proc_b)
            a_old, b_old = set(a_new), set(b_new)

            res_list = []
            for row in a_new:
                if row not in b_old:
                    res_list.append(('a',) + row)
            for row in b_new:
                if row not in a_old:
                    res_list.append(('b',) + row)
            if 'sort_order' in check:
                if isinstance(check['sort_order'], str):
                    check['sort_order'] = [check['sort_order']]
                try:
                    sort_order = [head.index(f.upper()) + 1 for f in check['sort_order']]
                except ValueError:
                    sort_order = check['sort_order']
                try:
                    res_list = sorted(res_list, key=itemgetter(*sort_order, 0))
                except (AttributeError, TypeError) as exc:
                    pass
            return [('порядок',) + head] + res_list

    return await run_check()
