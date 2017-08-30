import motor.motor_asyncio as motor

import settings as S

db = motor.AsyncIOMotorClient(S.db).get_default_database()
