import logging
import hashlib
import os
import math
import string
import secrets
from functools import partial

logger = logging.getLogger()



class Tools:
    def __init__(self, config, database):
        self.config = config
        self.database = database
        self.alphabet = string.ascii_letters + string.digits

    def md5sum(self, filename):
        with open(filename, mode='rb') as f:
            d = hashlib.md5()
            for buf in iter(partial(f.read, 4096), b''):
                d.update(buf)
        return d.hexdigest()

    def strip_base_path(self, fullpath, partpath):
        return os.path.relpath(fullpath, partpath)

    def strip_path(self, path):
        return os.path.basename(path)

    def strip_filename(self, path):
        return os.path.dirname(path)

    def ls_recursive(self, path):
        files = []
        # r=root, d=directories, f = files
        for r, d, f in os.walk(path):
            for file in f:
                    files.append(os.path.join(r, file))
        return files

    def convert_size(self, size_bytes):
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

    def create_encryption_key(self):
        return ''.join(secrets.choice(self.alphabet) for i in range(128))

    def create_filename_encrypted(self):
        filename_enc_helper = ''.join(secrets.choice(self.alphabet) for i in range(64))
        return "{}.enc".format(filename_enc_helper)
