from datetime import datetime
import logging
import logging.handlers
import os
import queue

import settings as S

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
    _log_queue = queue.Queue()
    queue_handler = logging.handlers.QueueHandler(_log_queue)
    # _log_handler = logging.StreamHandler()
    try:
        with open(S.LOG_NAME, 'r') as fd:
            pass
    except FileNotFoundError:
        dirname = os.path.split(S.LOG_NAME)[0]
        os.mkdir(dirname)

    _file_handler = logging.handlers.TimedRotatingFileHandler(S.LOG_NAME, when='midnight')

    _queue_listener = logging.handlers.QueueListener(_log_queue, _file_handler)
    _queue_listener.start()

    _format = '{asctime} {levelname:8s} --> {message}'
    logging.basicConfig(format=_format + '\n', style='{')

    _formatter = MSecFormatter(_format, datefmt='%H:%M:%S', style='{')
    # _log_handler.setFormatter(_formatter)
    _file_handler.setFormatter(_formatter)

    level = logging.DEBUG if S.DEBUG else logging.INFO

    def __del__(self):
        self._queue_listener.stop()

dummy = Dummy()

def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.addHandler(dummy.queue_handler)
    logger.setLevel(dummy.level)
    return StyleAdapter(logger)
