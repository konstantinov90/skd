import motor.motor_asyncio as motor

import settings
from . import aio

db = motor.AsyncIOMotorClient(
    settings.DATABASE, io_loop=aio.aio.get_event_loop()
).get_default_database()
