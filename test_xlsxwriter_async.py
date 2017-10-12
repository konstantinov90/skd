import asyncio
import xlsxwriter
from datetime import datetime

async def func():
    wb = xlsxwriter.Workbook('test.xlsx', {'default_date_format': 'dd-mm-yyyy HH:MM:SS'})
    ws = wb.add_worksheet('test_sheet')
    ws.write_row(0, 0, (123, 4322, 'dasdas', 'hello_worl', datetime.now()))
    await wb.close()

asyncio.get_event_loop().run_until_complete(func())