import logging
import subprocess
import os
import time

logger = logging.getLogger()

class Encryption:
    def __init__(self, config, database, tapelibrary, tools, local=False):
        self.config = config
        self.database = database
        self.tapelibrary = tapelibrary
        self.tools = tools
        self.local_files = local
        self.interrupted = False

    def set_interrupted(self):
        self.interrupted = True

    def encrypt_single_file(self, id, filepath, filename_enc):
        self.database.update_filename_enc(filename_enc, id)

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
            self.database.update_file_after_encrypt(filesize, encrypted_date, md5, id)

            if not self.local_files:
                time_started = time.time()
                os.remove(os.path.abspath("{}/{}".format(self.config['local-data-dir'], filepath)))
                logger.debug("Execution Time: Remove file after encryption: {} seconds".format(
                    time.time() - time_started))
        else:
            logger.warning("encrypt file failed, file: {} error: {}".format(id, openssl.stderr.readlines()))
            logger.debug(
                "Execution Time: Encrypt file with openssl: {} seconds".format(time.time() - time_started))


    def encrypt(self):
        logger.info("Starting encrypt files job")

        while True:
            files = self.database.get_files_to_be_encrypted()

            if len(files) == 0:
                break

            for file in files:
                logger.info("Processing: id: {}, filename: {}".format(file[0], file[1]))

                filename_enc = self.tools.create_filename_encrypted()
                while self.database.filename_encrypted_already_used(filename_enc):
                    logger.warning("Filename ({}) encrypted already exists, creating new one!".format(filename_enc))
                    filename_enc = self.tools.create_filename_encrypted()

                self.encrypt_single_file(file[0], file[2], filename_enc)

                if self.interrupted:
                    break

            if self.interrupted:
                break

        ## encrypt
        # openssl enc -aes-256-cbc -pbkdf2 -iter 100000 -in 'videofile.mp4' -out test.enc -k supersicherespasswort
        ## decrypt
        # openssl enc -d -aes-256-cbc -pbkdf2 -iter 100000 -in test.enc -out test.mp4

    def restore():
        ## TODO: Restore file by given name, path or encrypted name
        pass