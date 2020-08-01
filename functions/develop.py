import logging
import sys
import os
import subprocess
import threading
from lib import database
from tabulate import tabulate
from datetime import datetime


logger = logging.getLogger()


class Develop:
    def __init__(self, config, database, tapelibrary, tools, local=False):
        self.config = config
        self.database = database
        self.tapelibrary = tapelibrary
        self.tools = tools
        self.local_files = local
        self.interrupted = False
        self.downloaded_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.deleted_count = 0
        self.active_threads = []

    def current_test(self):

        if self.tapelibrary.get_current_lto_version() == 4:
            logger.info("LTO-4 Tape found, use tar for backup")
            print("LTO-4 Tape found, use tar for backup")

        print(self.tapelibrary.set_necessary_lto4_options())
        print(self.tapelibrary.get_current_blocksize())
        print(self.tapelibrary.set_blocksize())
        print(self.tapelibrary.get_current_block())
        print(self.tapelibrary.get_max_block())
        #print("test")
