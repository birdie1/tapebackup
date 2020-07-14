import logging
import datetime
import subprocess
import os
import sys
import time
import threading
from lib import database
from pathlib import Path

logger = logging.getLogger()


class Encryption:
    def __init__(self, config, engine, tapelibrary, tools, local=False):
        self.config = config
        self.engine = engine
        self.session = database.create_session(engine)
        self.tapelibrary = tapelibrary
        self.tools = tools
        self.local_files = local
        self.interrupted = False
        self.active_threads = []

    def set_interrupted(self):
        self.interrupted = True

    def encrypt_single_file_thread(self, threadnr, id, filepath, filename_enc):
        thread_session = database.create_session(self.engine)
        file = database.update_filename_enc(thread_session, id, filename_enc)

        time_started = time.time()

        if not self.local_files:
            command = ['openssl', 'enc', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in',
                       os.path.abspath(f"{self.config['local-data-dir']}/{filepath}"), '-out',
                       os.path.abspath(f"{self.config['local-enc-dir']}/{filename_enc}"), '-k',
                       self.config['enc-key']]
        else:
            command = ['openssl', 'enc', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in',
                       os.path.abspath(f"{self.config['local-base-dir']}/{filepath}"), '-out',
                       os.path.abspath(f"{self.config['local-enc-dir']}/{filename_enc}"), '-k',
                       self.config['enc-key']]
        openssl = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setpgrp)

        if openssl.returncode == 0:
            logger.debug(f"Execution Time: Encrypt file with openssl: {time.time() - time_started} seconds")

            time_started = time.time()
            md5 = self.tools.md5sum(os.path.abspath(f"{self.config['local-enc-dir']}/{filename_enc}"))
            logger.debug(f"Execution Time: md5sum encrypted file: {time.time() - time_started} seconds")

            filesize = os.path.getsize(os.path.abspath(f"{self.config['local-enc-dir']}/{filename_enc}"))
            encrypted_date = datetime.datetime.now()
            database.update_file_after_encrypt(thread_session, file, filesize, encrypted_date, md5)

            if not self.local_files:
                time_started = time.time()
                os.remove(os.path.abspath(f"{self.config['local-data-dir']}/{filepath}"))
                logger.debug(f"Execution Time: Remove file after encryption: {time.time() - time_started} seconds")
        else:
            logger.warning(f"encrypt file failed, file: {id} error: {openssl.stderr}")
            logger.debug(f"Execution Time: Encrypt file with openssl: {time.time() - time_started} seconds")

        self.active_threads.remove(threadnr)
        thread_session.close()

    def encrypt(self):
        logger.info("Starting encrypt files job")

        while True:
            files = database.get_files_to_be_encrypted(self.session)

            if len(files) == 0:
                break

            file_count_total = len(files)
            file_count_current = 0

            for file in files:
                file_count_current += 1
                for i in range(0, self.config['threads']['encrypt']):
                    if i not in self.active_threads:
                        next_thread = i
                        break

                logger.info(f"Starting Thread #{next_thread}, processing ({file_count_current}/{file_count_total}): "
                            f"id: {file.id}, filename: {file.filename}")

                filename_enc = self.tools.create_filename_encrypted()
                while database.filename_encrypted_already_used(self.session, filename_enc):
                    logger.warning(f"Filename ({filename_enc}) encrypted already exists, creating new one!")
                    filename_enc = self.tools.create_filename_encrypted()

                self.active_threads.append(next_thread)
                x = threading.Thread(target=self.encrypt_single_file_thread,
                                     args=(next_thread, file.id, file.path, filename_enc,),
                                     daemon=True)
                x.start()

                while threading.active_count() > self.config['threads']['encrypt']:
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
            return True

        openssl = [
            'openssl', 'enc', '-d', '-aes-256-cbc', '-pbkdf2', '-iter', '100000',
            '-in', str(src), '-out', str(dst), '-k', self.config['enc-key']
        ]

        try:
            subprocess.check_output(openssl, stderr=subprocess.STDOUT, preexec_fn=os.setpgrp)
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
