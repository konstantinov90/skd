"""DB module contains DBConnection classes and process_cursor decorator."""
# import decimal
# import copy
from contextlib import contextmanager
import cx_Oracle
# import settings as S

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

    def get_script_cursor(self, query, input_data):
        """get sql query cursor"""
        curs = self.con.cursor()
        curs.prepare(query)
        curs.execute(None, {i: v for i, v in input_data.items() if i.upper() in curs.bindnames()})
        return curs

    def script_cursor(self, query, get_field_names=False, **input_data):
        """get cursor generator"""

        tup = tuple

        cursor = self.get_script_cursor(query, input_data)
        if get_field_names:
            yield tuple(col[0] for col in cursor.description)

        while True:
            rows = cursor.fetchmany(ARRAYSIZE)
            if not rows:
                break
            yield from rows
        cursor.close()

    @contextmanager
    def cursor(self):
        """get cursor as context"""
        self.curs = self.con.cursor()
        yield self.curs
        self.curs.close()

    def exec_insert(self, query, **input_data):
        """execute pl/sql expression"""
        curs = self.con.cursor()
        curs.execute(query, input_data)
        curs.close()

    def exec_script(self, script, get_field_names=False, **input_data):
        """execute query and get results as a list"""
        query = script

        db_res = []

        curs = self.con.cursor()
        curs.prepare(query)

        curs.execute(None, {i: v for i, v in input_data.items() if i.upper() in curs.bindnames()})
        if get_field_names:
            db_res.append(tuple(col[0] for col in curs.description))
        for row in curs.fetchall():
            db_res.append(row)

        curs.close()
        return db_res

# def OraTypeHandler(cursor, name, defaultType, size, precision, scale):
#     if defaultType == cx_Oracle.NUMBER:
#         return cursor.var(str, 100, cursor.arraysize, outconverter=decimal.Decimal)

class OracleConnection(_DBConnection):
    """This class establishes connection to ORACLE DataBase."""
    def __init__(self, *conn_str):
        super().__init__()
        self.con = cx_Oracle.connect(*conn_str)
        # self.con.outputtypehandler = OraTypeHandler
