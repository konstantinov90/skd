import motor.motor_asyncio as motor

import settings as S

db = motor.AsyncIOMotorClient(S.db[0])[S.db[1]]
