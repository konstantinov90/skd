"""DB module contains DBConnection classes and process_cursor decorator."""
# import decimal
# import copy
from contextlib import contextmanager
from functools import partial

import cx_Oracle
# import settings as S

from . import aio

ARRAYSIZE = 1000

# def process_cursor(connection, script, **input_data):
#     """process_cursor decorator factory"""
#     def decorator(fun):
#         """decorator function"""
#         def dummy(*args):
#             """function to return"""
#             roller = RollerBar()
#             cursor = connection.get_script_cursor(script.get_query(), input_data)
#             row = cursor.fetchone()
#             while row:
#                 fun(row, *args)
#                 row = cursor.fetchone()
#                 roller.roll()
#             roller.finish()
#             cursor.close()
#         return dummy
#     return decorator
class async_cursor():
    def __init__(self, con):
        self.con = con
        self.curs = None

    async def __aenter__(self):
        self.curs = await aio.async_run(self.con.con.cursor)
        return self.curs

    async def __aexit__(self, exc_type, exc, tb):
        await aio.async_run(self.curs.close)

def filter_dict(dct, subdict):
    return {i: v for i, v in dct.items() if i.upper() in subdict}

class _DBConnection(object):
    """base DBConnection class"""
    def __init__(self):
        self.con = None
        self.curs = None

    def __del__(self):
        # release connection
        # print("closing connection...")
        self.con.close()
        # print("done!")

    def commit(self):
        """commit sql transaction"""
        self.con.commit()

    def rollback(self):
        """rollback sql transaction"""
        self.con.rollback()

    @contextmanager
    def cursor(self):
        """get cursor as context"""
        self.curs = self.con.cursor()
        yield self.curs
        self.curs.close()

    def async_cursor(self):
        return async_cursor(self)

    def exec_insert(self, query, **input_data):
        """execute pl/sql expression"""
        with self.cursor() as curs:
            curs.execute(query, input_data)

    def script_cursor(self, query, get_field_names=False, **input_data):
        """get cursor generator"""
        with self.cursor() as curs:
            curs.prepare(query)
            curs.execute(None, filter_dict(input_data, curs.bindnames()))
            if get_field_names:
                yield tuple(col[0] for col in curs.description)

            while True:
                rows = curs.fetchmany(ARRAYSIZE)
                if not rows:
                    break
                yield from rows

    async def async_script_cursor(self, query, get_field_names=False, **input_data):
        async with self.async_cursor() as curs:
            await aio.async_run(curs.prepare, query)
            args = filter_dict(input_data, await aio.async_run(curs.bindnames))
            await aio.async_run(curs.execute, None, args)
            if get_field_names:
                yield tuple(col[0] for col in curs.description)

            while True:
                rows = await aio.async_run(curs.fetchmany, ARRAYSIZE)
                if not rows:
                    break
                for row in rows:
                    yield row

    def exec_script(self, query, get_field_names=False, **input_data):
        """execute query and get results as a list"""
        db_res = []

        with self.cursor() as curs:
            curs.prepare(query)

            curs.execute(None, filter_dict(input_data, curs.bindnames()))
            if get_field_names:
                db_res.append(tuple(col[0] for col in curs.description))
            db_res += curs.fetchall()

        return db_res

    async def async_exec_script(self, query, get_field_names=False, **input_data):
        db_res = []

        async with self.async_cursor() as curs:
            await aio.async_run(curs.prepare, query)
            args = filter_dict(input_data, await aio.async_run(curs.bindnames))
            await aio.async_run(curs.execute, None, args)

            if get_field_names:
                db_res.append(tuple(col[0] for col in curs.description))
            db_res += await aio.async_run(curs.fetchall)

        return db_res



# def OraTypeHandler(cursor, name, defaultType, size, precision, scale):
#     if defaultType == cx_Oracle.NUMBER:
#         return cursor.var(str, 100, cursor.arraysize, outconverter=decimal.Decimal)


class OracleConnection(_DBConnection):
    """This class establishes connection to ORACLE DataBase."""
    @staticmethod
    async def get(*conn_str):
        con = OracleConnection(*conn_str, do_not_connect=True)
        await con._init()
        return con

    def __init__(self, *conn_str, **kwargs):
        super().__init__()
        self.con_dummy = partial(cx_Oracle.connect, *conn_str, threaded=True, encoding='utf-8')
        if 'do_not_connect' not in kwargs:
            self.con = self.con_dummy()
        # self.con.outputtypehandler = OraTypeHandler

    async def _init(self):
        self.con = await aio.async_run(self.con_dummy)
