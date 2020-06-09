import logging
import subprocess
import os
import time
import threading
from lib.database import Database
from tabulate import tabulate

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

    def start(self, file):
        ## TODO: Restore file by given name, path or encrypted name
        files = [1, 2]
        jobid = self.database.add_restore_job()
        self.database.add_restore_job_files(jobid, files)

        self.status()
        self.cont()

    def cont(self):
        pass

    def abort(self):
        pass

    def status(self, jobid=None):
        table = []
        stats_t = self.database.get_restore_job_stats_total(jobid)[0]
        stats_r = self.database.get_restore_job_stats_remaining(jobid)[0]
        table.append([
            "Total",
            stats_t[3],
            stats_t[4],
            stats_t[5]
        ])
        table.append([
            "Remaining",
            f"{stats_r[3]} ({stats_r[3]/stats_t[3]*100:.2f}%)",
            f"{stats_r[4]} ({stats_r[4]/stats_t[4]*100:.2f}%)",
            f"{stats_r[5]} ({stats_r[5]/stats_t[5]*100:.2f}%)"
        ])

        print(tabulate(table, headers=[
                '#',
                'Files',
                'Filesize',
                'Tapes'
            ], tablefmt='grid'))

    def list(self):
        pass
