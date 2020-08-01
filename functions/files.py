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
        return_list = []
        count = 0
        time_started = time.time()

        logger.info(f"Retrieving file list from server {self.config['remote-server']} directory {self.config['remote-data-dir']}")
        commands = ['ssh', self.config['remote-server'], f"find \"{self.config['remote-data-dir']}\" -type f"]
        process = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while True:
            output = process.stdout.readline().strip().decode("UTF-8")
            if output == '' and process.poll() is not None:
                break
            if output:
                count += 1
                return_list.append(output)
                if count % 10000 == 0:
                    logger.info(f"Entries found until now: {count}")
        rc = process.poll()

        if rc != 0:
            logger.error(f"Failed to retrieve filelist from remote server, error: {process.stderr}")
            logger.debug(f"Execution Time: Building filelist: {time.time() - time_started} seconds")
            sys.exit(1)
        else:
            logger.info(f"Got file list from server {self.config['remote-server']} directory '{self.config['remote-data-dir']}'")
            logger.info(f"Entries found: {count}")
            logger.debug(f"Execution Time: Building filelist: {time.time() - time_started} seconds")
            return return_list

    def get_remote_filelist_fom_file(self, file):
        with open(file) as f:
            lines = f.read().splitlines()
        return lines

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

    def get(self, given_file=None):
        """
        Get files from remote server or add local files into database
        :param given_file: Filename to read list of files from, otherwise it will be retrieved via find
        :return: Nothing
        """
        if given_file is not None:
            logger.info(f"Taking filelist from given file {given_file}")
            result = self.get_remote_filelist_fom_file(given_file)
        else:
            if self.local_files:
                logger.info(f"Retrieving file list from server LOCAL directory "
                            f"{os.path.abspath(self.config['local-data-dir'])}")
                result = self.tools.ls_recursive(os.path.abspath(self.config['local-data-dir']))
                data_dir = self.config['local-data-dir']
                base_dir = self.config['local-base-dir']
            else:
                result = self.get_remote_filelist()
                data_dir = self.config['remote-data-dir']
                base_dir = self.config['remote-base-dir']

        file_count_total = len(result)
        file_count_current = 0
        logger.info(f"Found {file_count_total} entries. Start to process.")

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
            logger.debug(f"Processing {fullpath}")

            if database.file_exists_by_path(self.session, relpath) is None:
                # Get next thread id
                for i in range(0, self.config['threads']['get']):
                    if i not in self.active_threads:
                        next_thread = i
                        break
                logger.info(f"Starting Thread #{next_thread}, processing "
                            f"({file_count_current}/{file_count_total}): {fullpath}")

                self.active_threads.append(next_thread)
                x = threading.Thread(target=self.get_thread,
                                     args=(next_thread, relpath, fullpath,),
                                     daemon=True)
                x.start()

                while threading.active_count() > self.config['threads']['get']:
                    time.sleep(0.2)

            else:
                logger.debug(f"File already downloaded, skipping {relpath}")
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
                if data_dir in f"{base_dir}/{file.path}":
                    still_exists = False
                    for fpath in result:
                        if fpath.strip() == f"{base_dir}/{file.path}":
                            still_exists = True
                            break

                    ## Set delete flag in database
                    if not still_exists:
                        logger.info(f"Set delete flag for file id: {file.id}")
                        self.deleted_count += 1
                        database.set_file_deleted(self.session, file)

        logger.info(f"Processing finished: downloaded: {self.downloaded_count}, skipped (already downloaded): "
                    f"{self.skipped_count}, failed: {self.failed_count}, deleted: {self.deleted_count}")

    table_format_verbose = [
        ('Id',                  lambda i: i.id),
        ('Duplicate Id',        lambda i: i.duplicate_id),
        ('Filename',            lambda i: i.filename),
        ('Path',                lambda i: i.path),
        ('Filename Encrypted',  lambda i: i.filename_encrypted),
        ('Modified Date',       lambda i: Tools.datetime_from_db(i.mtime)),
        ('Filesize',            lambda i: Tools.convert_size(i.filesize)),
        ('Filesize Encrypted',  lambda i: Tools.convert_size(i.filesize_encrypted)),
        ('md5sum',              lambda i: i.md5sum_file),
        ('md5sum Encrypted',    lambda i: i.md5sum_encrypted),
        ('Tape',                lambda i: "" if i.tape is None else i.tape.label),
        ('Downloaded Date',     lambda i: i.downloaded_date),
        ('Encrypted Date',      lambda i: i.encrypted_date),
        ('Written Date',        lambda i: i.written_date),
        ('Downloaded',          lambda i: i.downloaded),
        ('Encrypted',           lambda i: i.encrypted),
        ('Written',             lambda i: i.written),
        ('Verified Count',      lambda i: i.verified_count),
        ('Verified Last Date',  lambda i: i.verified_last),
        ('Deleted',             lambda i: i.deleted)
    ]

    table_format_short = [
        ('Id',              lambda i: i.id),
        ('Filename',        lambda i: i.filename),
        ('Modified Date',   lambda i: i.mtime),
        ('Filesize',        lambda i: Tools.convert_size(i.filesize)),
        ('Tape',            lambda i: "" if i.tape is None else i.tape.label)
    ]

    def list(self, path_filter, verbose=False, tape=None):
        if len(path_filter) == 0:
            if tape is None:
                files = database.get_all_files(self.session)
            else:
                files = database.get_files_like(self.session, tape=tape)
        else:
            print(Tools.wildcard_to_sql_many_sqlalchemy(path_filter))
            files = database.get_files_like(
                self.session,
                Tools.wildcard_to_sql_many_sqlalchemy(path_filter),
                tape
            )
        if verbose:
            format = self.table_format_verbose
        else:
            format = self.table_format_short
        Tools.table_print(files, format)

    table_format_duplicate = [
        ('Id',              lambda i: i.id),
        ('Orig. Id',        lambda i: i.file.id),
        ('Original Path',   lambda i: i.file.filename),
        ('Modified Date',   lambda i: i.mtime),
        ('Second Path',     lambda i: i.filename),
        ('Filesize',        lambda i: i.file.filesize),
    ]

    def duplicate(self):
        dup = database.list_duplicates(self.session)
        Tools.table_print(dup, self.table_format_duplicate)
        print(f"Duplicate files: {len(dup)}")

    def summary(self):
        table = []
        table.append(["Total File Count", database.get_file_count(self.session)])
        min_s = database.get_min_file_size(self.session)
        max_s = database.get_max_file_size(self.session)
        total_s = database.get_total_file_size(self.session)
        table.append(["Smallest File", "{} ({})".format(min_s, Tools.convert_size(min_s))])
        table.append(["Biggest File", "{} ({})".format(max_s, Tools.convert_size(max_s))])
        table.append(["Total File/Backup Size", "{} ({})".format(total_s, Tools.convert_size(total_s))])

        print(tabulate(table, headers=['Key', 'Value'], tablefmt='grid'))
