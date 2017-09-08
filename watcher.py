import sys
import time
import logging
from subprocess import Popen # , PIPE
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

# prc = None

class Restarter(PatternMatchingEventHandler):
    pyt = None
    def on_modified(self, event):
        super(Restarter, self).on_modified(event)
        what = 'directory' if event.is_directory else 'file'
        logging.info("Modified %s: %s", what, event.src_path)
        if self.pyt:
            self.pyt.terminate()
            logging.info("Python terminated")
        self.pyt = Popen(['python', 'run_server.py'])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    event_handler = Restarter(patterns=['*.py'])
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('watcher stopped')
        observer.stop()
    observer.join()
