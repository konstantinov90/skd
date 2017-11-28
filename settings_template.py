import os.path

DATABASE = 'mongodb://skd:123456@vm-ts-blk-app2/skd_cache'

CHECK_RESULT_PATH = 'check_results'

CHECK_REGISTRY_PATH = os.path.join(os.path.split(__file__)[0], 'reg')

HARD_PROCESSES = False

MAX_CONCURRENT_CHECKS = 20

CACHE_REFRESH_SECONDS = 5

LOG_PATH = 'logs'

DEBUG = True

PORT = 9000

REPOS = {
    "TS": 'git.rosenergo.com:u/konstantinov/SKD/TS',
    "TSIII": 'git.rosenergo.com:u/konstantinov/SKD/TSIII',
    "COMPARE": 'git.rosenergo.com:u/konstantinov/SKD/COMPARE',
    "BR": 'git.rosenergo.com:u/lina_che/SKD/BR',
    "NSS": 'git.rosenergo.com:u/konstantinov/SKD/NSS',
    "DOWNLOADS": 'git.rosenergo.com:u/konstantinov/SKD/DOWNLOADS',
}

KERBEROS_AUTH = 'username', 'pwd'
