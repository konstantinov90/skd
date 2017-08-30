db = 'mongodb://skd:123456@vm-ts-blk-app2/skd_cache'

check_result_path = 'check_results'

MAX_CONCURRENT_CHECKS = 20

CACHE_REFRESH_SECONDS = 5

LOG_NAME = r'logs/app.log'

DEBUG = False

PORT = 9000

REPOS = {
    "TS": 'git.rosenergo.com:u/konstantinov/SKD/TS',
    "TSIII": 'git.rosenergo.com:u/konstantinov/SKD/TSIII',
    "COMPARE": 'git.rosenergo.com:u/konstantinov/SKD/COMPARE',
    "BR": 'git.rosenergo.com:u/konstantinov/SKD/BR',
    "NSS": "https://github.com/konstantinov90/NSS_checks", # "git.rosenergo.com:u/konstantinov/SKD/NSS",
}
