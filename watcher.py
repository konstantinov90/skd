import logging
import os.path
import platform
import sys
import time
from subprocess import Popen # , PIPE

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

# prc = None

PROC_NAME = 'python{}'.format('3' if 'linux' in platform.system().lower() else '')

class Restarter(PatternMatchingEventHandler):
    pyt = Popen([PROC_NAME, 'run_server.py'])
    def on_modified(self, event):
        super(Restarter, self).on_modified(event)
        what = 'directory' if event.is_directory else 'file'
        logging.info("Modified %s: %s", what, event.src_path)
        self.pyt.terminate()
        logging.info("Python terminated")
        self.pyt = Popen([PROC_NAME, 'run_server.py'])


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    root = '.'
    event_handler = Restarter(patterns=['*.py'], ignore_patterns=[os.path.join(root, 'reg', '*')])
    observer = Observer()
    observer.schedule(event_handler, root, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('watcher stopped')
        observer.stop()
    observer.join()
