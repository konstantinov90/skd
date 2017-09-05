import datetime
import platform
from subprocess import Popen, PIPE

import settings
from utils import app_log
from utils.aio import aio

LOG = app_log.get_logger(__name__)

FIVE_HOURS = datetime.timedelta(seconds=5).seconds

if 'linux' in platform.system().lower():
    username, pwd = settings.KERBEROS_AUTH
    def kinit():
        proc = Popen(['kinit', username], stdout=PIPE, stdin=PIPE, stderr=PIPE, universal_newlines=True)
        out, err = proc.communicate(input='{}\n'.format(pwd))
        if err:
            raise Exception(err)
        LOG.info(out)
        aio.call_at(aio.get_event_loop().time() + FIVE_HOURS, kinit)
else:
    def kinit():
        pass
