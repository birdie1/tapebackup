import logging
import hashlib
import os
from functools import partial

logger = logging.getLogger()



class Tools:
    def __init__(self, config, database):
        self.config = config
        self.database = database

    def md5sum(self, filename):
        with open(filename, mode='rb') as f:
            d = hashlib.md5()
            for buf in iter(partial(f.read, 4096), b''):
                d.update(buf)
        return d.hexdigest()

    def strip_base_path(self, fullpath):
        return os.path.relpath(fullpath, self.config['remote-base-dir'])

    def strip_path(self, path):
        return os.path.basename(path)

    def strip_filename(self, path):
        return os.path.dirname(path)