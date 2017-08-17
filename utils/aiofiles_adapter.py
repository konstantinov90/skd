import aiofiles

class Adapter(object):
    def __init__(self):
        self.lines = ''

    def write(self, line):
        self.lines += line
