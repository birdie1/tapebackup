import logging
import time
import os
import subprocess
import threading
from tabulate import tabulate
from datetime import datetime
from lib.database import Database

logger = logging.getLogger()

class Files:
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
        self.active_threads = []

    def set_interrupted(self):
        self.interrupted = True

    def get_remote_filelist(self):
        time_started = time.time()

        logger.info("Retrieving file list from server '{}' directory '{}'".format(self.config['remote-server'],
                                                                                  self.config['remote-data-dir']))
        commands = ['ssh', self.config['remote-server'], 'find "{}" -type f'.format(self.config['remote-data-dir'])]
        ssh = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = ssh.stdout.readlines()
        logger.info(
            "Got file list from server {} directory '{}'".format(self.config['remote-server'], self.config['remote-data-dir']))

        logger.debug("Execution Time: Building filelist: {} seconds".format(time.time() - time_started))
        return result

    def get_thread(self, threadnr, id, filename, fullpath, relpath, directory):
        downloaded = False
        thread_db = Database(self.config)

        if not self.local_files:
            os.makedirs("{}/{}".format(self.config['local-data-dir'], directory), exist_ok=True)

            time_started = time.time()
            command = ['rsync', '--protect-args', '-ae', 'ssh', '{}:{}'.format(self.config['remote-server'], fullpath),
                       '{}/{}'.format(self.config['local-data-dir'], directory)]
            rsync = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setpgrp)
            #child_process_pid.append(rsync.pid)

            if len(rsync.stderr.readlines()) == 0:
                #child_process_pid.remove(rsync.pid)
                downloaded = True
            logger.debug("Execution Time: Downloading file: {} seconds".format(time.time() - time_started))

        if self.local_files or downloaded:
            time_started = time.time()
            if self.local_files:
                mtime = int(os.path.getmtime(os.path.abspath("{}/{}".format(self.config['local-base-dir'], relpath))))
                md5 = self.tools.md5sum(os.path.abspath("{}/{}".format(self.config['local-base-dir'], relpath)))
                filesize = os.path.getsize(os.path.abspath("{}/{}".format(self.config['local-base-dir'], relpath)))
            else:
                mtime = int(os.path.getmtime(os.path.abspath("{}/{}".format(self.config['local-data-dir'], relpath))))
                md5 = self.tools.md5sum(os.path.abspath("{}/{}".format(self.config['local-data-dir'], relpath)))
                filesize = os.path.getsize(os.path.abspath("{}/{}".format(self.config['local-data-dir'], relpath)))

            time_delta = time.time() - time_started
            logger.debug("Execution Time: Building md5sum and mtime: {} seconds".format(time_delta))

            downloaded_date = int(time.time())
            duplicate = thread_db.get_files_by_md5(md5)
            if len(duplicate) > 0:
                logger.info("File downloaded with another name. Storing filename in Database: {}".format(filename))
                duplicate_id = duplicate[0][0]
                inserted_id = thread_db.insert_alternative_file_names(filename, relpath, duplicate_id, downloaded_date)
                thread_db.delete_broken_db_entry(id)
                if not self.local_files:
                    time_started = time.time()

                    os.remove(os.path.abspath("{}/{}".format(self.config['local-data-dir'], relpath)))

                    time_delta = time.time() - time_started
                    logger.debug("Execution Time: Remove duplicate file: {} seconds".format(time_delta))
                self.skipped_count += 1
            else:
                thread_db.update_file_after_download(filesize, mtime, downloaded_date, md5, 1, id)
                self.downloaded_count += 1
                logger.debug("Download finished: {}".format(relpath))
        else:
            logger.warning("Download failed, file: {} error: {}".format(relpath, rsync.stderr.readlines()))
            self.failed_count += 1

        self.active_threads.remove(threadnr)

    def get(self):
        if self.local_files:
            logger.info(
                "Retrieving file list from server LOCAL directory '{}'".format(os.path.abspath(self.config['local-data-dir'])))
            result = self.tools.ls_recursive(os.path.abspath(self.config['local-data-dir']))
        else:
            result = self.get_remote_filelist()

        file_count_total = len(result)
        logger.info("Found {} entries. Start to process.".format(file_count_total))

        for fpath in result:
            if self.tools.calculate_over_max_storage_usage(-1):
                while threading.active_count() > 1:
                    time.sleep(1)
                logger.warning("max-storage-size reached, exiting!")
                break
            if isinstance(fpath, bytes):
                fullpath = fpath.decode("UTF-8").rstrip()
                relpath = self.tools.strip_base_path(fullpath, self.config['remote-base-dir'])
            else:
                fullpath = fpath.rstrip()
                relpath = self.tools.strip_base_path(fullpath, self.config['local-base-dir'])
            logger.debug("Processing {}".format(fullpath))

            filename = self.tools.strip_path(fullpath)
            directory = self.tools.strip_filename(relpath)

            if not self.database.check_if_file_exists_by_path(relpath):
                for i in range(0, self.config['threads']):
                    if i not in self.active_threads:
                        next_thread = i
                        break
                logger.info("Starting Thread #{}, processing: {}".format(next_thread, fullpath))
                id = self.database.insert_file(filename, relpath)
                logger.debug("Inserting file into database. Fileid: {}".format(id))

                self.active_threads.append(next_thread)
                x = threading.Thread(target=self.get_thread,
                                     args=(next_thread, id, filename, fullpath, relpath, directory,),
                                     daemon=True)
                x.start()

                while threading.active_count() > self.config['threads']:
                    time.sleep(0.2)

            else:
                logger.debug("File already downloaded, skipping {}".format(relpath))
                self.skipped_count += 1

            if self.interrupted:
                while threading.active_count() > 1:
                    time.sleep(1)
                break

        logger.info(
            "Processing finished: downloaded: {}, skipped (already downloaded): {}, failed: {}".format(self.downloaded_count,
                                                                                                       self.skipped_count,
                                                                                                       self.failed_count))


    def list(self, short):
        table = []
        files = self.database.get_all_files()
        if not short:
            for i in files:
                table.append([
                    i[0],
                    i[1],
                    i[2],
                    i[3],
                    datetime.utcfromtimestamp(int(i[4])).strftime('%Y-%m-%d %H:%M:%S') if i[4] is not None else "",
                    self.tools.convert_size(i[5]) if i[5] is not None else "",
                    self.tools.convert_size(i[6]) if i[6] is not None else "",
                    i[7],
                    i[8],
                    i[9],
                    datetime.utcfromtimestamp(int(i[10])).strftime('%Y-%m-%d %H:%M:%S') if i[10] is not None else "",
                    datetime.utcfromtimestamp(int(i[11])).strftime('%Y-%m-%d %H:%M:%S') if i[11] is not None else "",
                    datetime.utcfromtimestamp(int(i[12])).strftime('%Y-%m-%d %H:%M:%S') if i[12] is not None else "",
                    i[13],
                    i[14],
                    i[15],
                    i[16],
                    datetime.utcfromtimestamp(int(i[17])).strftime('%Y-%m-%d %H:%M:%S') if i[17] is not None else "",
                    i[18]
                ])
            print(tabulate(table, headers=[
                'Id',
                'Filename',
                'Path',
                'Filename Encrypted',
                'Modified Date',
                'Filesize'
                'Filesize Encrypted',
                'md5sum',
                'md5sum Encrypted',
                'Tape',
                'Downloaded Date',
                'Encrypted Date',
                'Written Date',
                'Downloaded',
                'Encrypted',
                'Written',
                'Verified Count',
                'Verified Last Date',
                'Deleted'
            ], tablefmt='grid'))
        else:
            for i in files:
                if i[18] == 0:
                    table.append([
                        i[0],
                        i[1],
                        datetime.utcfromtimestamp(int(i[4])).strftime('%Y-%m-%d %H:%M:%S') if i[4] is not None else "",
                        self.tools.convert_size(i[5]) if i[5] is not None else "",
                        i[9]
                    ])
            print(tabulate(table, headers=[
                'Id',
                'Path',
                'Modified Date',
                'Filesize'
                'Tape'
            ], tablefmt='grid'))



    def duplicate(self):
        table = []
        dup = self.database.list_duplicates()
        for i in dup:
            table.append([
                i[0],
                datetime.utcfromtimestamp(int(i[1])).strftime('%Y-%m-%d %H:%M:%S'),
                i[2],
                i[3]
            ])
        print(tabulate(table, headers=['Original Name', 'Modified Date', 'Second Name', 'Filesize'], tablefmt='grid'))