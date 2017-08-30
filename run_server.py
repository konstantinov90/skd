from utils import app_log
LOG = app_log.get_logger(__name__)

try:
    import server
except Exception:
    LOG.exception('fatal error')
    raise
