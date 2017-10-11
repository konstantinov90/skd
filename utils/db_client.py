import weakref

import motor.motor_asyncio as motor

import settings
from utils import aio

cons = {} # weakref.WeakValueDictionary()

def get_db(loop=None):
    loop = loop or aio.aio.get_event_loop()

    if loop not in cons:
        cons[loop] = motor.AsyncIOMotorClient(settings.DATABASE, io_loop=loop).get_default_database()
    return cons[loop]
