import motor.motor_asyncio as motor

import settings as S
from . import aio

db = motor.AsyncIOMotorClient(S.db, io_loop=aio.aio.get_event_loop()).get_default_database()
