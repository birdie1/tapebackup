import errno
import logging
import hashlib
import os
import re
import math
import string
import secrets
import tarfile
import xattr
from functools import partial
from datetime import datetime
from tabulate import tabulate
from pathlib import Path

logger = logging.getLogger()

class Tools:
    def __init__(self, config, ):
        self.config = config
        #self.database = database
        self.alphabet = string.ascii_letters + string.digits

    @staticmethod
    def _md5sum(reader):
        d = hashlib.md5()
        for buf in iter(partial(reader.read, 4096), b''):
            d.update(buf)
        return d.hexdigest()

    @classmethod
    def md5sum(cls, filename):
        with open(filename, mode='rb') as f:
            return cls._md5sum(f)

    @classmethod
    def md5sum_tar(cls, archive):
        with tarfile.open(archive, mode='r|') as t:
            f = t.extractfile(t.next())
            return cls._md5sum(f)

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
        if self.config['max_storage_usage'] == '' or self.config['max_storage_usage'] is None:
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

    @staticmethod
    def wildcard_to_sql(string):
        return string.replace('*', '%')

    @staticmethod
    def wildcard_to_sqlalchemy(string):
        return string.replace('*', '')

    @classmethod
    def wildcard_to_sql_many(cls, strings):
        return [cls.wildcard_to_sql(s) for s in strings]\

    @classmethod
    def wildcard_to_sql_many_sqlalchemy(cls, strings):
        return [cls.wildcard_to_sqlalchemy(s) for s in strings]

    @staticmethod
    def table_format_entry(format, file):
        return (formatter(file) for header,formatter in format)

    @classmethod
    def table_print(cls, rows, format):
        data = (cls.table_format_entry(format, row) for row in rows)
        headers = (header for header,formatter in format)
        table = tabulate(data, headers=headers, tablefmt='grid')
        print(table)

    def order_by_startblock(self, files):
        start_and_files = list()
        for file in files:
            src = Path(self.config['local-tape-mount-dir']) / file.filename_encrypted

            try:
                start_str = xattr.getxattr(src.resolve(), 'ltfs.startblock')
                start = int(start_str)
            except OSError as e:
                if e.errno == errno.ENODATA:
                    logging.debug(f'No xattrs available for {file.filename_encrypted}, falling back to inode ordering')
                    stat_result = src.stat()
                    start = stat_result.st_ino
                else:
                    raise

            logger.debug(f'{src} starts at {start}')
            start_and_files.append((start, file))

        return [y for x,y in sorted(start_and_files, key=lambda i: i[0])]

    @staticmethod
    def count_files_fit_on_tape(filelist, free_space):
        """
        Get a count of files that will be written to tape
        :param filelist: list of files that could be written to tape
        :param free_space: current free space minus preserved on target tape
        :return: Integer: count of files
        """
        cur_space = 0
        count = 0
        for file in filelist:
            if (cur_space + file.filesize) < free_space:
                cur_space += file.filesize
                count += 1
            else:
                return count
        return count
