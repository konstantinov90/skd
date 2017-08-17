db = 'vm-ts-blk-app2', 'skd_cache'

check_result_path = 'check_results'

MAX_CONCURRENT_CHECKS = 20

CACHE_REFRESH_SECONDS = 5

STATIC_URL = 'http://ats-konstantin:80/static'

class IMAGES():
    INFO = '<img width="50px" src="{}/images/Emblem-important-yellow.svg.png" alt="3"/>'.format(STATIC_URL)
    FAIL = '<img width="50px" src="{}/images/x.png" alt="2"/>'.format(STATIC_URL)
    PASS = '<img width="45px" src="{}/images/checkmark-xxl.png" alt="1"/>'.format(STATIC_URL)
    ROTTEN = '<img width="50px" src="{}/images/rotten_apple.png" alt="1"/>'.format(STATIC_URL)
    FRESH = '<img width="50px" src="{}/images/green_apple.png" alt="2"/>'.format(STATIC_URL)
    DOCUMENT = '<img width="50px" src="{}/images/document.png" alt="1"/>'.format(STATIC_URL)
    XLSX = '<img width="50px" src="{}/images/xlsx.png" alt="2"/>'.format(STATIC_URL)

    @classmethod
    def file(cls, filename):
        if filename.endswith('xlsx'):
            return cls.XLSX
        return cls.DOCUMENT
