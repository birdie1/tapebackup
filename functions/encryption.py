import logging
import subprocess
import os
import sys
import time
import threading
from lib.database import Database
from pathlib import Path

logger = logging.getLogger()


class Encryption:
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

    def encrypt_single_file_thread(self, threadnr, id, filepath, filename_enc):
        thread_db = Database(self.config)

        thread_db.update_filename_enc(filename_enc, id)

        time_started = time.time()

        if not self.local_files:
            command = ['openssl', 'enc', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in',
                       os.path.abspath('{}/{}'.format(self.config['local-data-dir'], filepath)), '-out',
                       os.path.abspath('{}/{}'.format(self.config['local-enc-dir'], filename_enc)), '-k',
                       self.config['enc-key']]
        else:
            command = ['openssl', 'enc', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in',
                       os.path.abspath('{}/{}'.format(self.config['local-base-dir'], filepath)), '-out',
                       os.path.abspath('{}/{}'.format(self.config['local-enc-dir'], filename_enc)), '-k',
                       self.config['enc-key']]
        openssl = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   preexec_fn=os.setpgrp)

        if len(openssl.stderr.readlines()) == 0:
            logger.debug(
                "Execution Time: Encrypt file with openssl: {} seconds".format(time.time() - time_started))

            time_started = time.time()
            md5 = self.tools.md5sum(os.path.abspath("{}/{}".format(self.config['local-enc-dir'], filename_enc)))
            logger.debug("Execution Time: md5sum encrypted file: {} seconds".format(time.time() - time_started))

            filesize = os.path.getsize(os.path.abspath('{}/{}'.format(self.config['local-enc-dir'], filename_enc)))
            encrypted_date = int(time.time())
            thread_db.update_file_after_encrypt(filesize, encrypted_date, md5, id)

            if not self.local_files:
                time_started = time.time()
                os.remove(os.path.abspath("{}/{}".format(self.config['local-data-dir'], filepath)))
                logger.debug("Execution Time: Remove file after encryption: {} seconds".format(
                    time.time() - time_started))
        else:
            logger.warning("encrypt file failed, file: {} error: {}".format(id, openssl.stderr.readlines()))
            logger.debug(
                "Execution Time: Encrypt file with openssl: {} seconds".format(time.time() - time_started))

        self.active_threads.remove(threadnr)

    def encrypt(self):
        logger.info("Starting encrypt files job")

        while True:
            files = self.database.get_files_to_be_encrypted()

            if len(files) == 0:
                break

            file_count_total = len(files)
            file_count_current = 0

            for file in files:
                file_count_current += 1
                for i in range(0, self.config['threads']):
                    if i not in self.active_threads:
                        next_thread = i
                        break

                logger.info("Starting Thread #{}, processing ({}/{}): id: {}, filename: {}".format(
                    next_thread, file_count_current, file_count_total, file[0], file[1]
                ))

                filename_enc = self.tools.create_filename_encrypted()
                while self.database.filename_encrypted_already_used(filename_enc):
                    logger.warning("Filename ({}) encrypted already exists, creating new one!".format(filename_enc))
                    filename_enc = self.tools.create_filename_encrypted()

                self.active_threads.append(next_thread)
                x = threading.Thread(target=self.encrypt_single_file_thread,
                                     args=(next_thread, file[0], file[2], filename_enc,),
                                     daemon=True)
                x.start()

                while threading.active_count() > self.config['threads']:
                    time.sleep(0.2)

                if self.interrupted:
                    while threading.active_count() > 1:
                        time.sleep(1)
                    break

            if self.interrupted:
                while threading.active_count() > 1:
                    time.sleep(1)
                break

            ## Multithreading fix: Wait for all threads to finish, otherwise one file get encrypted twice!
            while threading.active_count() > 1:
                time.sleep(1)

    # src relative to tape, dst relative to restore-dir
    def decrypt_relative(self, src, dst, mkdir=False):
        if 'restore-dir' not in self.config:
            logging.error('"restore-dir" not configured')
            sys.exit(1)
        restore_dir = Path(self.config['restore-dir'])

        if not restore_dir.is_dir():
            logging.error(f'restore directory "{restore_dir}" does not exist or is not a directory')
            sys.exit(1)

        src_path = Path(self.config['local-tape-mount-dir']) / src
        dst_path = restore_dir / dst

        if mkdir:
            dst_path.parent.mkdir(parents=True, exist_ok=True)

        return self.decrypt(src_path.resolve(), dst_path.resolve())

    def decrypt(self, src, dst):
        if not isinstance(dst, Path):
            dst = Path(dst)
        if dst.is_file():
            logger.error(f'File {dst} already exists, skipping decrypt')
            return False

        openssl = [
            'openssl', 'enc', '-d', '-aes-256-cbc', '-pbkdf2', '-iter', '100000',
            '-in', str(src), '-out', str(dst), '-k', self.config['enc-key']
        ]

        try:
            subprocess.check_output(openssl, stderr=subprocess.STDOUT)
            return True
        except subprocess.CalledProcessError as e:
            logging.error(f'Decryption failed: {e.stdout.decode("utf-8").splitlines()[0]}')
            if dst.is_file() and dst.stat().st_size == 0:
                dst.unlink()
            return False

## encrypt
# openssl enc -aes-256-cbc -pbkdf2 -iter 100000 -in 'videofile.mp4' -out test.enc -k supersicherespasswort
## decrypt
# openssl enc -d -aes-256-cbc -pbkdf2 -iter 100000 -in test.enc -out test.mp4
