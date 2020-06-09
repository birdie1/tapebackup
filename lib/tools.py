import logging
import hashlib
import os
import re
import math
import string
import secrets
from functools import partial
from datetime import datetime

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

    @staticmethod
    def convert_size(size_bytes):
        if size_bytes is None or size_bytes == 0:
            return "0 B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

    def back_convert_size(self, size):
        units = {"B": 1, "K": 2 ** 10, "M": 2 ** 20, "G": 2 ** 30, "T": 2 ** 40, "P": 2 ** 50, "E": 2 ** 60}

        m = re.search('(\d*)\s*(\w*)', size)
        number = m.group(1)
        if m.group(2):
            unit = m.group(2)
        else:
            unit = "B"
        return int(float(number) * units[unit])

    def create_encryption_key(self):
        return ''.join(secrets.choice(self.alphabet) for i in range(128))

    def create_filename_encrypted(self):
        filename_enc_helper = ''.join(secrets.choice(self.alphabet) for i in range(64))
        return "{}.enc".format(filename_enc_helper)

    def folder_size(self, path):
        total = 0
        for entry in os.scandir(path):
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += self.folder_size(entry.path)
        return total

    def calculate_over_max_storage_usage(self, new_file_size):
        if self.config['max_storage_usage'] == '' or self.config['max_storage_usage'] == None:
            return False
        current_size = self.folder_size(self.config['local-data-dir']) \
                       + self.folder_size(self.config['local-enc-dir']) \
                       + self.folder_size(self.config['local-verify-dir'])

        if new_file_size == -1:
            if current_size >= self.back_convert_size(self.config['max_storage_usage']):
                return True
            else:
                return False
        else:
            if (current_size + new_file_size) >= self.back_convert_size(self.config['max_storage_usage']):
                return True
            else:
                return False

    @staticmethod
    def datetime_from_db(field):
        if field is not None:
            return datetime.utcfromtimestamp(int(field)).strftime('%Y-%m-%d %H:%M:%S')
        else:
            return ""
