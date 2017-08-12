import aiohttp.web as web
import motor.motor_asyncio as motor

db = motor.AsyncIOMotorClient('vm-ts-blk-app2').skd_cache
checks = db.checks

app = web.Application()

async def index(request):
    return web.Response(text='SKD rest api')

app.router.add_get('/', index)

async def receive_task(request):
    task = await request.json()
    return web.json_response(task)

app.router.add_post('/send_task', receive_task)

async def get_checks(request):
    query = await request.json()
    resp = await checks.find_one(query)
    resp['_id'] = str(resp['_id'])
    resp['task_id'] = str(resp['task_id'])
    resp['started'] = str(resp['started'])
    resp['finished'] = str(resp['finished'])
    return web.json_response(resp)

app.router.add_post('/get_checks', get_checks)

web.run_app(app, port=9000)
