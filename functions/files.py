import datetime
import logging
import time
import os
import sys
import subprocess
import threading
from tabulate import tabulate
from lib import database
from lib.tools import Tools
from lib.models import File, Tape, RestoreJob, RestoreJobFileMap

logger = logging.getLogger()


class Files:
    def __init__(self, config, engine, tapelibrary, tools, local=False):
        self.config = config
        self.engine = engine
        self.session = database.create_session(engine)
        self.tapelibrary = tapelibrary
        self.tools = tools
        self.local_files = local
        self.interrupted = False
        self.downloaded_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.deleted_count = 0
        self.active_threads = []

    def set_interrupted(self):
        self.interrupted = True

    def get_remote_filelist(self):
        time_started = time.time()

        logger.info("Retrieving file list from server '{}' directory '{}'".format(self.config['remote-server'],
                                                                                  self.config['remote-data-dir']))
        commands = ['ssh', self.config['remote-server'], f"find \"{self.config['remote-data-dir']}\" -type f"]
        ssh = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if ssh.returncode != 0:
            logger.error(f"Failed to retrieve filelist from remote server, error: {ssh.stderr}")
            sys.exit(1)
        else:
            logger.info(
                f"Got file list from server {self.config['remote-server']} directory '{self.config['remote-data-dir']}'")
            logger.debug(f"Execution Time: Building filelist: {time.time() - time_started} seconds")
            return ssh.stdout.decode("UTF-8").split('\n')

    def get_thread(self, threadnr, relpath, fullpath):
        """
        Thread which will download and insert file into database
        :param threadnr: thread number
        :param relpath: relative file path
        :param fullpath: absolut filepath on the remote server (Or local absolut filepath)
        :return:
        """
        downloaded = False
        filename = self.tools.strip_path(fullpath)
        directory = self.tools.strip_filename(relpath)
        thread_session = database.create_session(self.engine)

        file = database.insert_file(thread_session, filename, relpath)
        logger.debug("Inserting file into database. Fileid: {}".format(file.id))

        if not self.local_files:
            try:
                os.makedirs(f"{self.config['local-data-dir']}/{directory}", exist_ok=True)
            except OSError as e:
                logger.error(f"Failed to create local folder ({self.config['local-data-dir']}/{directory}), exiting: {e.errno}: {e.strerror}")
                self.interrupted = True
                return False

            time_started = time.time()
            command = ['rsync', '--protect-args', '-ae', 'ssh', f"{self.config['remote-server']}:{fullpath}",
                       f"{self.config['local-data-dir']}/{directory}"]
            rsync = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setpgrp)
            if rsync.returncode == 0:
                downloaded = True
            else:
                logger.warning("Download failed, file: {} error: {}".format(file.path, rsync.stderr))
                self.failed_count += 1
            logger.debug(f"Execution Time: Downloading file: {time.time() - time_started} seconds")

        if self.local_files or downloaded:
            time_started = time.time()
            if self.local_files:
                mtime = datetime.datetime.fromtimestamp(int(os.path.getmtime(os.path.abspath(f"{self.config['local-base-dir']}/{file.path}"))))
                md5 = self.tools.md5sum(os.path.abspath(f"{self.config['local-base-dir']}/{file.path}"))
                filesize = os.path.getsize(os.path.abspath(f"{self.config['local-base-dir']}/{file.path}"))
            else:
                mtime = datetime.datetime.fromtimestamp(int(os.path.getmtime(os.path.abspath(f"{self.config['local-data-dir']}/{file.path}"))))
                md5 = self.tools.md5sum(os.path.abspath(f"{self.config['local-data-dir']}/{file.path}"))
                filesize = os.path.getsize(os.path.abspath(f"{self.config['local-data-dir']}/{file.path}"))

            logger.debug(f"Execution Time: Building md5sum and mtime: {time.time() - time_started} seconds")

            downloaded_date = datetime.datetime.now()
            file_dup = database.get_file_by_md5(thread_session, md5)
            if file_dup is None:
                database.update_file_after_download(thread_session, file, filesize, mtime, downloaded_date, md5)
                self.downloaded_count += 1
                logger.debug("Download finished: {}".format(file.path))
            else:
                logger.info(f"File downloaded with another name. Storing filename in Database: {file.filename}")
                database.update_duplicate_file_after_download(thread_session, file, file_dup, mtime, downloaded_date)
                if not self.local_files:
                    time_started = time.time()
                    os.remove(os.path.abspath("{}/{}".format(self.config['local-data-dir'], file.path)))
                    logger.debug(f"Execution Time: Remove duplicate file: {time.time() - time_started} seconds")
                self.skipped_count += 1

        self.active_threads.remove(threadnr)
        thread_session.close()

    def get(self):
        """
        Get files from remote server or add local files into database
        :return: Nothing
        """
        if self.local_files:
            logger.info(
                "Retrieving file list from server LOCAL directory '{}'".format(os.path.abspath(self.config['local-data-dir'])))
            result = self.tools.ls_recursive(os.path.abspath(self.config['local-data-dir']))
            data_dir = self.config['local-data-dir']
            base_dir = self.config['local-base-dir']
        else:
            result = self.get_remote_filelist()
            data_dir = self.config['remote-data-dir']
            base_dir = self.config['remote-base-dir']

        file_count_total = len(result)
        file_count_current = 0
        logger.info("Found {} entries. Start to process.".format(file_count_total))

        for fpath in result:
            # Check if max-storage-size from config file is reached
            file_count_current += 1
            if self.tools.calculate_over_max_storage_usage(-1):
                while threading.active_count() > 1:
                    time.sleep(1)
                logger.warning("max-storage-size reached, exiting!")
                break

            if fpath.strip() == "":
                continue
            fullpath = fpath.strip()
            relpath = self.tools.strip_base_path(fullpath, base_dir)
            logger.debug("Processing {}".format(fullpath))

            if database.file_exists_by_path(self.session, relpath) is None:
                # Get next thread id
                for i in range(0, self.config['threads']['get']):
                    if i not in self.active_threads:
                        next_thread = i
                        break
                logger.info("Starting Thread #{}, processing ({}/{}): {}".format(next_thread, file_count_current, file_count_total, fullpath))

                self.active_threads.append(next_thread)
                x = threading.Thread(target=self.get_thread,
                                     args=(next_thread, relpath, fullpath,),
                                     daemon=True)
                x.start()

                while threading.active_count() > self.config['threads']['get']:
                    time.sleep(0.2)

            else:
                logger.debug("File already downloaded, skipping {}".format(relpath))
                self.skipped_count += 1

            if self.interrupted:
                while threading.active_count() > 1:
                    time.sleep(1)
                break

        ## Detect deleted files
        if not self.interrupted:
            self.deleted_count = 0
            files = database.get_not_deleted_files(self.session)
            for file in files:
                ## Only look for files in the data path (then you can still specify subfolder instead of syncing all)
                if data_dir in "{}/{}".format(base_dir, file.path):
                    still_exists = False
                    for fpath in result:
                        if fpath.strip() == "{}/{}".format(base_dir, file.path):
                            still_exists = True

                    ## Set delete flag in database
                    if not still_exists:
                        logger.info(f"Set delete flag for file id: {file.id}")
                        self.deleted_count += 1
                        database.set_file_deleted(self.session, file)

        logger.info(f"Processing finished: downloaded: {self.downloaded_count}, skipped (already downloaded): "
                    f"{self.skipped_count}, failed: {self.failed_count}, deleted: {self.deleted_count}")
















    table_format_verbose = [
        ('Id',                  lambda i: i[0]),
        ('Filename',            lambda i: i[1]),
        ('Path',                lambda i: i[2]),
        ('Filename Encrypted',  lambda i: i[3]),
        ('Modified Date',       lambda i: Tools.datetime_from_db(i[4])),
        ('Filesize',            lambda i: Tools.convert_size(i[5])),
        ('Filesize Encrypted',  lambda i: Tools.convert_size(i[6])),
        ('md5sum',              lambda i: i[7]),
        ('md5sum Encrypted',    lambda i: i[8]),
        ('Tape',                lambda i: i[9]),
        ('Downloaded Date',     lambda i: Tools.datetime_from_db(i[10])),
        ('Encrypted Date',      lambda i: Tools.datetime_from_db(i[11])),
        ('Written Date',        lambda i: Tools.datetime_from_db(i[12])),
        ('Downloaded',          lambda i: i[13]),
        ('Encrypted',           lambda i: i[14]),
        ('Written',             lambda i: i[15]),
        ('Verified Count',      lambda i: i[16]),
        ('Verified Last Date',  lambda i: Tools.datetime_from_db(i[17])),
        ('Deleted',             lambda i: i[18])
    ]

    table_format_short = [
        ('Id',              lambda i: i[0]),
        ('Filename',        lambda i: i[1]),
        ('Modified Date',   lambda i: Tools.datetime_from_db(i[4])),
        ('Filesize',        lambda i: Tools.convert_size(i[5])),
        ('Tape',            lambda i: i[9])
    ]

    def list(self, path_filter, verbose=False, tape=None):
        if len(path_filter) == 0:
            if tape is None:
                files = self.database.get_all_files()
            else:
                files = self.database.get_files_like(tape=tape)
        else:
            files = self.database.get_files_like(
                Tools.wildcard_to_sql_many(path_filter),
                tape
            )
        if verbose:
            format = self.table_format_verbose
        else:
            format = self.table_format_short
        Tools.table_print(files, format)

    table_format_duplicate = [
        ('Original Name',   lambda i: i[0]),
        ('Modified Date',   lambda i: Tools.datetime_from_db(i[1])),
        ('Second Name',     lambda i: i[2]),
        ('Filesize',        lambda i: i[3]),
    ]

    def duplicate(self):
        dup = self.database.list_duplicates()
        Tools.table_print(dup, self.table_format_duplicate)

    def summary(self):
        table = []
        table.append(["Total File Count", self.database.get_file_count()])
        min_s = self.database.get_min_file_size()
        max_s = self.database.get_max_file_size()
        total_s = self.database.get_total_file_size()
        table.append(["Smallest File", "{} ({})".format(min_s, Tools.convert_size(min_s))])
        table.append(["Biggest File", "{} ({})".format(max_s, Tools.convert_size(max_s))])
        table.append(["Total File/Backup Size", "{} ({})".format(total_s, Tools.convert_size(total_s))])

        print(tabulate(table, headers=['Key', 'Value'], tablefmt='grid'))
