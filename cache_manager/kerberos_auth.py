import datetime
import platform
from subprocess import Popen, PIPE

import settings
from utils import app_log
from utils.aio import aio

LOG = app_log.get_logger()

FIVE_HOURS = datetime.timedelta(hours=5).seconds

if 'linux' in platform.system().lower():
    username, pwd = settings.KERBEROS_AUTH
    def kinit():
        proc = Popen(['kinit', username], stdout=PIPE, stdin=PIPE, stderr=PIPE, universal_newlines=True)
        out, err = proc.communicate(input='{}\n'.format(pwd))
        if err:
            raise Exception(err)
        LOG.info(out)
        loop = aio.get_event_loop()
        loop.call_at(loop.time() + FIVE_HOURS, kinit)
else:
    def kinit():
        pass
