import motor.motor_asyncio as motor

import settings
from utils import aio

cons = {}

def get_db():
    loop = aio.aio.get_event_loop()

    return cons.setdefault(loop, motor.AsyncIOMotorClient(settings.DATABASE, io_loop=loop).get_default_database())
