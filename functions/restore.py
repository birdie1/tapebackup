import logging
import subprocess
import os
import time
import threading
from tabulate import tabulate

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
        self.active_threads = []

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
        jobid = self.database.add_restore_job()
        self.database.add_restore_job_files(jobid, file_ids)

        print(f"Restore job {jobid} created:")
        self.status()
        self.cont()

    def cont(self):
        pass

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
    ]

    def status(self, jobid=None, verbose=False):
        table = []
        stats_t = self.database.get_restore_job_stats_total(jobid)[0]
        stats_r = self.database.get_restore_job_stats_remaining(jobid)[0]
        table_data = [list(stats_t) + ["Total"]]
        table_data += [[None]*3 + [
            f"{stats_r[3]} ({stats_r[3]/stats_t[3]*100:.2f}%)",
            f"{stats_r[4]} ({stats_r[4]/stats_t[4]*100:.2f}%)",
            f"{stats_r[5]} ({stats_r[5]/stats_t[5]*100:.2f}%)",
            "Remaining"
        ]]
        Tools.table_print(table_data, self.table_format_status)

        if verbose:
            jobid = stats_t[0]
            files = self.database.get_restore_job_files(jobid)
            Tools.table_print(files, self.table_format_status_files)

    def read_filelist(self, filelist):
        with open(filelist, "r") as f:
            return [l.rstrip("\n") for l in f]

    # get file ids for a list of files from the database,
    # warn if some do not exist and optionally filter by a tape name
    def resolve_file_ids(self, files, tape=None):
        db_files = self.database.get_files_like(files, tape, items=["id,path"])
        for file in files:
            # don't check wildcard files
            if '%' in file:
                continue
            if not any(path == file for id, path in db_files):
                logger.warning("File {file} not found")
        return [id for id,file in db_files]
