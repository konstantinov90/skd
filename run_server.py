#! /usr/bin/env python3
from utils import app_log
LOG = app_log.get_logger()

try:
    import server
except Exception:
    LOG.exception('fatal error')
    raise
