from datetime import datetime
import logging
import logging.handlers
import os
import queue

import settings as S

from utils import authorization as auth

__all__ = ('get_logger',)

class Message(object):
    def __init__(self, fmt, args):
        self.fmt = fmt
        self.args = args

    def __str__(self):
        return self.fmt.format(*self.args)

class StyleAdapter(logging.LoggerAdapter):
    def __init__(self, logger, extra=None):
        super().__init__(logger, extra or {})

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            msg, kwargs = self.process(msg, kwargs)
            self.logger._log(level, Message(msg, args), (), **kwargs)

class MSecFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        created = datetime.fromtimestamp(record.created)
        if datefmt:
            tme = created.strftime(datefmt)
            msg = "%s.%03d" % (tme, record.msecs)
        else:
            msg = super().formatTime(record, datefmt)
        return msg

class Dummy(object):
    level = logging.DEBUG if S.DEBUG else logging.INFO
    instances = {}
    def __init__(self, log_filename):
        _log_queue = queue.Queue()
        self.queue_handler = logging.handlers.QueueHandler(_log_queue)
        # _log_handler = logging.StreamHandler()

        try:
            with open(log_filename, 'r') as fd:
                pass
        except FileNotFoundError:
            dirname = os.path.split(log_filename)[0]
            try:
                os.mkdir(dirname)
            except FileExistsError:
                pass

        _file_handler = logging.handlers.TimedRotatingFileHandler(log_filename, when='midnight')

        self._queue_listener = logging.handlers.QueueListener(_log_queue, _file_handler)
        self._queue_listener.start()

        _format = '{asctime} {levelname:8s} --> {message}'
        logging.basicConfig(format=_format + '\n', style='{')

        _formatter = MSecFormatter(_format, datefmt='%H:%M:%S', style='{')
        # _log_handler.setFormatter(_formatter)
        _file_handler.setFormatter(_formatter)
        self.instances[log_filename] = self

    def __del__(self):
        self._queue_listener.stop()

    @classmethod
    def get_dummy(cls, log_filename):
        return cls.instances.get(log_filename, cls(log_filename))


def get_logger(logger_name, log_filename=S.LOG_NAME):
    logger = logging.getLogger(logger_name)
    dummy = Dummy.get_dummy(log_filename)
    logger.addHandler(dummy.queue_handler)
    logger.setLevel(dummy.level)
    return StyleAdapter(logger)


async def access_log_middleware(app, handler):
    access_log = get_logger('access_log', r'logs/access.log')

    async def _middleware_handler(request):
        username = await auth.auth.get_auth(request)
        access_log.info('{}: {}', username, request)
        access_log.debug('{!r}', request['body'])

        response = await handler(request)

        try:
            resp = response.text
        except AttributeError:
            resp = type(response)
        access_log.debug('{}: {} response: {}', username, request, resp)
        access_log.debug('{}', response.headers)
        return response

    return _middleware_handler
