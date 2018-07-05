"""DB module contains DBConnection classes and process_cursor decorator."""
import decimal
# import copy
from contextlib import contextmanager
from functools import partial, wraps
import re
import sys

import cx_Oracle
import psycopg2
import vertica_python

from utils import aio

##################### MONKEY PATCHING #####################

def _parse_numeric(s):
    try:
        return int(s)
    except ValueError:
        return float(s)

def _numeric_conv(converter):
    @wraps(converter)
    def _wrapper(*args, **kwargs):
        parsers = converter()
        i = [datatype for datatype, parser in parsers].index('numeric')
        return tuple(parsers[:i] + [('numeric', _parse_numeric)] + parsers[i + 1:])
    return _wrapper

vertica_python.vertica.column.Column._data_type_conversions = _numeric_conv(vertica_python.vertica.column.Column._data_type_conversions)

############################################################

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
class _AsyncCursor():
    def __init__(self, con):
        self.con = con
        self.curs = None

    async def __aenter__(self):
        self.curs = await aio.async_run(self.con.con.cursor)
        return self.curs

    async def __aexit__(self, exc_type, exc, tb):
        await aio.async_run(self.curs.close)

class _DBConnection(object):
    """base DBConnection class"""
    @classmethod
    async def get(cls, *args, **kwargs):
        con = cls(*args, **kwargs, do_not_connect=True)
        await con._init()
        return con

    async def _init(self):
        self.con = await aio.async_run(self.con_func)
        if hasattr(self.con, 'outputtypehandler'):
            self.con.outputtypehandler = OraTypeHandler

    def __init__(self):
        self.con = None
        self.con_func = None

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
        curs = self.con.cursor()
        yield curs
        curs.close()

    def async_cursor(self):
        return _AsyncCursor(self)

    def exec_insert(self, query, **input_data):
        """execute pl/sql expression"""
        with self.cursor() as curs:
            curs.execute(query, input_data)

    @staticmethod
    def _run_query(curs, query, input_data):
        curs.execute(query, input_data)

    @staticmethod
    async def _async_run_query(curs, query, input_data):
        func = partial(curs.execute, query, **input_data)
        await aio.async_run(func)

    def script_cursor(self, query, get_field_names=False, **input_data):
        """get cursor generator"""
        with self.cursor() as curs:
            self._run_query(curs, query, input_data)

            if get_field_names:
                yield tuple(col[0] for col in curs.description)

            while True:
                rows = curs.fetchmany(ARRAYSIZE)
                if not rows:
                    break
                yield from rows

    async def async_script_cursor(self, query, get_field_names=False, **input_data):
        async with self.async_cursor() as curs:
            await self._async_run_query(curs, query, input_data)

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
            self._run_query(curs, query, input_data)

            if get_field_names:
                db_res.append(tuple(col[0] for col in curs.description))
            db_res += curs.fetchall()

        return db_res

    async def async_exec_script(self, query, get_field_names=False, **input_data):
        db_res = []

        async with self.async_cursor() as curs:
            await self._async_run_query(curs, query, input_data)

            if get_field_names:
                db_res.append(tuple(col[0] for col in curs.description))
            db_res += await aio.async_run(curs.fetchall)

        return db_res

def OraNumConverter(val):
    try:
        return int(val)
    except ValueError:
        return decimal.Decimal(val.replace(',', '.'))

def OraTypeHandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.NUMBER:
        return cursor.var(str, 100, cursor.arraysize, outconverter=OraNumConverter)

def filter_dict(dct, subdict):
    return {i: v for i, v in dct.items() if i.upper() in subdict}

def Connection(class_name, *args, **kwargs):
    return getattr(sys.modules[__name__], class_name)(*args, **kwargs)

def AsyncConnection(class_name):
    return getattr(sys.modules[__name__], class_name)

class OracleConnection(_DBConnection):
    """This class establishes connection to ORACLE DataBase."""
    def __init__(self, *args, **kwargs):
        super().__init__()
        do_not_connect = kwargs.pop('do_not_connect', False)
        kwargs.setdefault('threaded', True)
        if int(cx_Oracle.version[0]) >= 6:
            kwargs.setdefault('encoding', 'windows-1251')
        self.con_func = partial(cx_Oracle.connect, *args, **kwargs)
        if not do_not_connect:
            self.con = self.con_func()
        if self.con:
            self.con.outputtypehandler = OraTypeHandler

    @staticmethod
    def _run_query(curs, query, input_data):
        curs.prepare(query)
        curs.execute(None, filter_dict(input_data, curs.bindnames()))

    @staticmethod
    async def _async_run_query(curs, query, input_data):
        await aio.async_run(curs.prepare, query)
        bindnames = await aio.async_run(curs.bindnames)
        await aio.async_run(curs.execute, None, filter_dict(input_data, bindnames))

class VerticaConnection(_DBConnection):
    """This class establishes connection to Vertica DataBase."""
    def __init__(self, **kwargs):
        super().__init__()
        do_not_connect = kwargs.pop('do_not_connect', False)
        self.con_func = partial(vertica_python.connect, **kwargs)
        if not do_not_connect:
            self.con = self.con_func()

class PostgresConnection(_DBConnection):
    """This class establishes connection to POSTGRESQL DataBase"""
    def __init__(self, *args, **kwargs):
        super().__init__()
        do_not_connect = kwargs.pop('do_not_connect', False)
        self.con_func = partial(psycopg2.connect, *args, **kwargs)
        if not do_not_connect:
            self.con = self.con_func()

    @staticmethod
    def _run_query(curs, query, input_data):
        _query = re.sub(r'(?<!\:)\:([a-zA-Z0-9]+)', r'{\1}', query)
        curs.execute(_query.format(**input_data))

    @staticmethod
    async def _async_run_query(curs, query, input_data):
        _query = re.sub(r'(?<!\:)\:([a-zA-Z0-9]+)', r'{\1}', query)
        await aio.async_run(curs.execute, _query.format(**input_data))
