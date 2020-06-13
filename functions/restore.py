import logging
import subprocess
import os
import time
import threading
import xattr
from tabulate import tabulate
from collections import OrderedDict
from pathlib import Path

from lib.database import Database
from lib.tools import Tools

logger = logging.getLogger()


class Restore:
    def __init__(self, config, database, tapelibrary, tools, local=False):
        self.config = config
        self.database = database
        self.tapelibrary = tapelibrary
        self.tools = tools
        self.local_files = local
        self.interrupted = False
        self.encryption = Encryption(config, database, tapelibrary, tools, local)
        self.active_threads = []
        self.jobid = None

    def set_interrupted(self):
        self.interrupted = True

    def start(self, files, tape=None, filelist=""):
        ## TODO: Restore file by given name, path or encrypted name
        if files is None:
            files = []
        files = Tools.wildcard_to_sql_many(files)
        if filelist:
            files += self.read_filelist(filelist)

        file_ids = self.resolve_file_ids(files, tape)
        self.jobid = self.database.add_restore_job()
        self.database.add_restore_job_files(self.jobid, file_ids)

        print(f"Restore job {self.jobid} created:")
        self.status()
        self.cont()

    table_format_next_tapes = [
        ('Tape',            lambda i: i[0]),
        ('# Files',         lambda i: i[1]),
        ('Remaining Size',  lambda i: Tools.convert_size(i[2])),
    ]

    # continue one round of a given restore job
    # if no job id is given, use the latest job
    # one round consists of:
    #   1) query the library for available tapes
    #   2) get a list of all files to restore from these tapes
    #   3) restore the files to the configured target directory
    #   4) determine a list of tapes to load for the next round
    #      and prompt the user to load these
    def cont(self, jobid=None):
        if jobid is None:
            self.set_latest_job()
        else:
            self.jobid = jobid

        tag_in_tapelib, tags_to_remove_from_library = self.tapelibrary.get_tapes_tags_from_library()
        tapes = tag_in_tapelib + tags_to_remove_from_library

        files = self.database.get_restore_job_files(self.jobid, tapes, restored=False)
        if files:
            self.restore_files(files)
        else:
            logger.info("No files to restore on the loaded tapes")

        next_tapes = self.make_next_tapes_info(self)
        if next_tapes:
            Tools.table_print(next_tapes, table_format_next_tapes)
        else:
            logger.info("No more files to restore. Restore job complete.")
            self.database.set_restore_job_finished(self.jobid)

    def abort(self):
        pass

    table_format_list = [
        ('Job ID',          lambda i: i[0]),
        ('Started',         lambda i: i[1]),
        ('Remaining Files', lambda i: i[3]),
        ('Remaining Size',  lambda i: i[4]),
    ]

    def list(self):
        stats_r = self.database.get_restore_job_stats_remaining()
        Tools.table_print(stats_r, self.table_format_list)

    table_format_status = [
        ('#',           lambda i: i[-1]),
        ('Files',       lambda i: i[3]),
        ('Filesize',    lambda i: i[4]),
        ('Tapes',       lambda i: i[5]),
    ]

    table_format_status_files = [
        ('Filename',    lambda i: i[1]),
        ('Filesize',    lambda i: i[3]),
        ('Tape',        lambda i: i[4]),
        ('Restored',    lambda i: 'Yes' if i[5] else 'No'),
    ]

    def status(self, jobid=None, verbose=False):
        if jobid is None:
            self.set_latest_job()
        else:
            self.jobid = jobid

        table = []
        stats_t = self.database.get_restore_job_stats_total(self.jobid)[0]
        stats_r = self.database.get_restore_job_stats_remaining(self.jobid)[0]
        table_data = [list(stats_t) + ["Total"]]
        table_data += [[None]*3 + [
            f"{stats_r[3]} ({stats_r[3]/stats_t[3]*100:.2f}%)",
            f"{stats_r[4]} ({stats_r[4]/stats_t[4]*100:.2f}%)",
            f"{stats_r[5]} ({stats_r[5]/stats_t[5]*100:.2f}%)",
            "Remaining"
        ]]
        Tools.table_print(table_data, self.table_format_status)

        if verbose:
            files = self.database.get_restore_job_files(self.jobid, restored=True)
            Tools.table_print(files, self.table_format_status_files)

    def read_filelist(self, filelist):
        with open(filelist, "r") as f:
            return [l.rstrip("\n") for l in f]

    def set_latest_job(self):
        self.jobid, _ = self.database.get_latest_restore_job()
        if self.jobid is None:
            logging.error("No restore job available")
            sys.exit(1)

    # get file ids for a list of files from the database,
    # warn if some do not exist and optionally filter by a tape name
    def resolve_file_ids(self, files, tape=None):
        db_files = self.database.get_files_like(files, tape,
            items=['id', 'path'], written=True)
        for file in files:
            # don't check wildcard files
            if '%' in file:
                continue
            if not any(path == file for id, path in db_files):
                logger.warning("File {file} not found")
        return [id for id,file in db_files]

    # restores a list of files from database
    def restore_files(self, files):
        logger.debug(f'Restoring to {restore_dir}')
        tapes_files = self.group_files_by_tape(files)

        for tape, files in tapes_files.items():
            self.restore_from_tape(tape, files)

    def restore_from_tape(self, tape, files):
        logger.info(f'Restoring from tape {tape}')
        self.tapelibrary.load(tape)
        self.tapelibrary.ltfs()

        ordered_files = self.order_by_startblock(files)
        for file in ordered_files:
            self.restore_single_file(self, file[0], file[2], file[6])

        logger.info(f'Restoring from tape {tape} done')
        self.tapelibrary.unload()

    # returns a dictionary containing {tape: (n_files, files_size)}
    def make_next_tapes_info(self):
        files = self.database.get_restore_job_files(self.jobid, restored=False)
        tapes = dict()
        for _, _, _, size, tape, _ in files:
            info = (tapes[tape][0] + 1, tapes[tape][1] + size) \
                    if tape in tapes else (1, size)
            tapes[tape] = info
        return tapes

    def group_files_by_tape(self, files):
        grouped = dict()
        for file in files:
            tape = file[4]
            args = file[:4] + file[5:]
            grouped[tape] = args
        return grouped

    def order_by_startblock(self, files):
        ordered_files = OrderedDict()
        for file in files:
            filename_encrypted = file[6]
            src = Path(self.config['local-tape-mount-dir']) / filename_encrypted

            # from ltfs_ordered_copy
            start_str = xattr.get(src, 'ltfs.startblock')
            start = int(start_str)

            logger.debug(f'{src} starts at {start}')
            ordered_files[start] = file

        return ordered_files

    def restore_single_file(self, file_id, path, filename_encrypted):
        success = self.encryption.decrypt_relative(filename_encrypted, path)
        if success:
            logger.info(f'Restored {path} successfully')
            self.database.set_file_restored(self.jobid, file_id)
        else:
            logger.error(f'Restoring {path} failed')
