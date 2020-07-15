import datetime
import logging
import subprocess
import os
import sys
import time
import random
import shutil
from lib import database
logger = logging.getLogger()


class Tape:
    def __init__(self, config, engine, tapelibrary, tools, local=False):
        self.config = config
        self.engine = engine
        self.session = database.create_session(engine)
        self.tapelibrary = tapelibrary
        self.tools = tools
        self.local_files = local
        self.interrupted = False

    def set_interrupted(self):
        self.interrupted = True

    def info(self):
        print(f"Loaderinfo from Device {self.config['devices']['tapelib']}:")
        for i in self.tapelibrary.loaderinfo():
            print(f"    {i.decode('utf-8').strip()}")

        print("")
        print(f"Tapeinfo from Device {self.config['devices']['tapedrive']}:")
        for i in self.tapelibrary.tapeinfo():
            print(f"    {i.decode('utf-8').strip()}")

        print("")
        print(f"MTX Info from Device {self.config['devices']['tapelib']}:")
        for i in self.tapelibrary.mtxinfo():
            print(f"    {i.decode('utf-8').strip()}")

    def status(self):
        tapes, tapes_to_remove = self.tapelibrary.get_tapes_tags_from_library()

        try:
            lto_whitelist = False
            if self.config['lto-whitelist'] is not None:
                lto_whitelist = True
        except KeyError:
            lto_whitelist = False

        if not lto_whitelist:
            print(f"Ignored Tapes ({len(self.config['lto-blacklist'])}): {self.config['lto-blacklist']}")
        else:
            print(f"Whitelisted Tapes ({len(self.config['lto-whitelist'])}): {self.config['lto-whitelist']}")

        full = []
        for i in database.get_full_tapes():
            if lto_whitelist:
                if i[0] not in self.config['lto-whitelist']:
                    continue
            full.append(i[0])
        print(f"Full tapes: {full}")

        print("")
        print(f"Free tapes in library({len(tapes)}): {tapes}")

        print("")
        print(f"Please remove following tapes from library ({len(tapes_to_remove)}): {tapes_to_remove}")

    def filecount_from_verify_files_config(self, filelist):
        if type(self.config['verify-files']) == int:
            return self.config['verify-files']
        else:
            return int(len(filelist) * int(self.config['verify-files'][0:self.config['verify-files'].index("%")]) / 100)

    def test_backup_pieces_ltfs(self, filelist, filecount_to_test):
        logger.info(f"Testing {filecount_to_test} files md5sum")

        files = self.tools.order_by_startblock(random.choices(filelist, k=filecount_to_test))

        for file in files:
            logger.info(f"Testing md5sum of file {file.filename}")
            if self.tools.md5sum(f"{self.config['local-tape-mount-dir']}/{filename_encrypted}") != file.md5sum_encrypted:
                logger.info(f"md5sum of {file.id}:{file.filename} is wrong: exiting!")
                return False

            if self.interrupted:
                break
        return True

    def test_backup_pieces_tar(self, filelist, filecount_to_test):
        logger.info("Testing {} files md5sum".format(filecount_to_test))

        files = sorted(random.choices(filelist, k=filecount_to_test), key=lambda i: i.tapeposition)

        for file in files:
            logger.info(f"Testing md5sum of file {file.filename}")

            self.tapelibrary.seek(file.tapeposition)
            if self.tools.md5sum_tar(self.config['devices']['tapedrive']) != file.md5sum_encrypted:
                logger.info(f"md5sum of {file.id}:{file.filename} is wrong: exiting!")
                return False

            if self.interrupted:
                break
        return True

    def revert_ltfs_on_error_28(self, free, tape):
        logger.error("Tapedevice reports full filesystem. I will revert all files marked as written on this tape and "
                     "force format this device!")
        logger.error(f"Last reported free size was: {free} ({self.tools.convert_size(free)}), now it shows full!")
        logger.error("This can have different reasons, including a broken drive head. If you are using HPE drives, you "
                     "can use the tool 'HPE Library and Tape Tools' to check your drive.")
        logger.error("If it happens more often, you should consider to activate the 'tape-keep-free' option in "
                     "config.yml (Set it higher than than your last reported free size from debug output)")
        logger.error(f"Reverting {len(database.get_files_by_tapelabel(self.session, tape))} file entries.")
        database.revert_written_to_tape_by_label(self.session, tape)
        self.tapelibrary.force_mkltfs()
        logger.error("Revert files and force format device finished. Exiting now!")
        sys.exit(1)

    def write_file_ltfs(self, file, free, tape, count, filecount):
        logger.debug(f"Tape: Free: {free}, Fileid: {file.id}, Filesize: {file.encrypted_filesize}")

        logger.info(f"Writing file to tape ({count}/{filecount}): {file.filename}")
        time_started = time.time()
        try:
            shutil.copy2(
                f"{self.config['local-enc-dir']}/{file.filename_encrypted}",
                f"{self.config['local-tape-mount-dir']}/"
            )
        except OSError as error:
            if error.errno == 28:
                # This means no space left on device
                self.revert_ltfs_on_error_28(free, tape)
            else:
                logger.error(f"Unknown OS Error '{error}', exiting!")
                logger.error(f"You have now stale file entries in database and maybe a broken LTFS, you need to "
                             f"manually format this tape and set written=0, written_date=NULL and tape=NULL on files "
                             f"which has this tape '{tape}' assigned")
        logger.debug(f"Execution Time: Copy file to tape: {time.time() - time_started} seconds")
        database.update_file_after_write(self.session, file, datetime.datetime.now(), tape)

    def write_file_tar(self, filelist, free, tape):
        tape_position = self.tapelibrary.get_current_block()
        ids = []
        filesizes = 0
        filenames_enc = []
        for file in filelist:
            ids.append(file.id)
            filesizes += file.encrypted_filesize
            filenames_enc.append(file.filename_encrypted)

        logger.debug(f"Tape: Free: {free}, Fileid: {ids}, Filesize: {filesizes}")

        time_started = time.time()
        count = 0
        for file in filelist:
            count += 1
            logger.info(f"Writing file to tape ({count}/{len(filelist)} in this tar archive): {file.filename}")

        # Write file or filelist to tape with tar
        commands = ['tar', '-c', '-b128', '-f', self.config['devices']['tapedrive'], '-C', self.config['local-enc-dir']]
        commands.extend(filenames_enc)
        tar = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setpgrp)

        if tar.returncode != 0:
            logger.error(f"Failed writing tar to tape, manual check is required. Returncode: {tar.returncode}, "
                         f"Error: {std_err}")
            sys.exit(1)

        logger.debug(f"Execution Time: Copy files via tar to tape: {time.time() - time_started} seconds")
        for file in filelist:
            database.update_file_after_write(self.session, file, datetime.datetime.now(), tape, tape_position)
        new_tape_position = self.tapelibrary.get_current_block()
        database.update_tape_end_position(self.session, tape, new_tape_position)

    def tape_is_full_ltfs(self, tape, free):
        # For LTO-5 and above with LTFS support
        logger.warning(f"Tape is full ({self.tools.convert_size(free)} left): I am testing now a few media, writing "
                       f"summary into database and unloading tape")

        files = database.get_files_by_tapelabel(self.session, tape)
        if not self.test_backup_pieces_ltfs(files, self.filecount_from_verify_files_config(files)):
            logger.error(
                "md5sum on tape not equal to database. Stopping everything. Need manual check of the tape!")
            logger.error(f"If you do not use this tape anymore, or want to write all data again, you need to manual "
                         f"modify database. These file IDs were written to tape: "
                         f"{database.get_files_by_tapelabel(self.session, tape)}")
            return False

        database.mark_tape_as_full(self.session, tape, datetime.datetime.now(), len(files))

        ## WRITE Database encrypted on tape
        time_started = time.time()
        dt = int(time.time())
        command = ['openssl', 'enc', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in', self.config['database'],
                   '-out', f"{self.config['local-tape-mount-dir']}/tapebackup_{dt}.db.enc", '-k', self.config['enc-key']]
        openssl = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if openssl.returncode != 0:
            logger.error("Writing Database to Tape failed")
            return False

        ## WRITE Textfile containing (encryped_name|original_fullpath) of all files encrypted to tape
        with open('tapebackup_{}.txt'.format(dt), 'w') as f:
            for file in database.get_files_by_tapelabel(self.session, tape):
                f.write('"{}";"{}";"{}"\n'.format(file.id, file.path, file.filename_encrypted))
        command = ['openssl', 'enc', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in', f'tapebackup_{dt}.txt',
                   '-out', f"{self.config['local-tape-mount-dir']}/tapebackup_{dt}.txt.enc", '-k', self.config['enc-key']]
        openssl = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if openssl.returncode != 0:
            logger.error("Writing Textfile to Tape failed")
            return False
        os.remove(f"tapebackup_{dt}.txt")
        logger.debug(f"Execution Time: Encrypt and write databse to tape: {time.time() - time_started} seconds")

        ## DELETE all Files, that has been transfered to tape
        time_started = time.time()
        count = 1
        to_delete = database.get_files_by_tapelabel(self.session, tape)
        for file in to_delete:
            if os.path.exists("{}/{}".format(self.config['local-enc-dir'], file.filename_encrypted)):
                logger.info(f"Deleting encrypted file ({count}/{len(to_delete)}): {file.filename_encrypted} ({file.filename})")
                os.remove("{}/{}".format(self.config['local-enc-dir'], file.filename_encrypted))
            count += 1
        logger.debug(f"Execution Time: Deleted encrypted files written to tape: {time.time() - time_started} seconds")

        ## Unload tape
        self.tapelibrary.unload()
        return True

    def tape_is_full_tar(self, tape, free):
        ## For LTO-4
        logger.warning("Tape is full: I am testing now a few media, writing summary into database and unloading tape")

        files = database.get_files_by_tapelabel(self.session, tape)
        if not self.test_backup_pieces_tar(files, self.filecount_from_verify_files_config(files)):
            logger.error(
                "md5sum on tape not equal to database. Stopping everything. Need manual check of the tape!")
            logger.error(f"If you do not use this tape anymore, or want to write all data again, you need to manual "
                         f"modify database. These file IDs were written to tape: "
                         f"{database.get_files_by_tapelabel(self.session, tape)}")
            return False

        ## TODO: Write DATABASE and stuff to file, see tape_is_full_ltfs
        #database.mark_tape_as_full(self.session, tape, datetime.datetime.now(), len(files))

    def write(self):
        full = False
        tapes, tapes_to_remove = self.tapelibrary.get_tapes_tags_from_library()
        if len(tapes_to_remove) > 0:
            logger.warning(f"These tapes are full, please remove from library: {tapes_to_remove}")

        if len(tapes) == 0:
            logger.error(f"No free Tapes in Library, but you can remove these full ones: {tapes_to_remove}")
            return

        next_tape = tapes.pop(0)
        logger.info(f"Using tape {next_tape} for writing")
        self.tapelibrary.load(next_tape)
        lto_version = self.tapelibrary.get_current_lto_version()

        if lto_version >= 5:
            logger.info(f"LTO-{lto_version} Tape found, use LTFS for backup")
            ## Mount and maybe format tapedevice
            self.tapelibrary.ltfs()

            ## Write used tape into database
            database.write_tape_into_database(self.session, next_tape)

            time_started = time.time()
            st = os.statvfs(self.config['local-tape-mount-dir'])
            logger.info("Tape: Used: {} ({} GB), Free: {} ({} GB), Total: {} ({} GB)".format(
                (st.f_blocks - st.f_bfree) * st.f_frsize,
                int((st.f_blocks - st.f_bfree) * st.f_frsize / 1024 / 1024 / 1024),
                (st.f_bavail * st.f_frsize),
                int((st.f_bavail * st.f_frsize) / 1024 / 1024 / 1024),
                (st.f_blocks * st.f_frsize),
                int((st.f_blocks * st.f_frsize) / 1024 / 1024 / 1024)
            ))
            logger.debug(f"Execution Time: Getting tape space info: {time.time() - time_started} seconds")

            if "%" in str(self.config['tape-keep-free']):
                tape_keep_free = int(st.f_blocks * st.f_frsize *
                                     int(self.config['tape-keep-free'][0:self.config['tape-keep-free'].index("%")]) /
                                     100)
            else:
                tape_keep_free = self.tools.back_convert_size(str(self.config['tape-keep-free']))
            logger.debug(f"Keep {tape_keep_free} ({self.tools.convert_size(tape_keep_free)}) free on tape given by config file!")

            files = database.get_files_to_be_written(self.session)
            filecount = self.tools.count_files_fit_on_tape(files, ((st.f_bavail * st.f_frsize) - tape_keep_free))
            count = 1
            for file in files:
                st = os.statvfs(self.config['local-tape-mount-dir'])
                free = (st.f_bavail * st.f_frsize)

                ## Check if enough space on tape, otherwise unmount and use next tape
                if file.encrypted_filesize > (free - tape_keep_free):
                    full = self.tape_is_full_ltfs(next_tape, free)
                    break
                else:
                    self.write_file_ltfs(file, free, next_tape, count, filecount)
                    count += 1

                if self.interrupted:
                    break

        elif lto_version == 4:
            logger.info("LTO-4 Tape found, use tar for backup")
            self.tapelibrary.set_necessary_lto4_options()
            self.tapelibrary.set_blocksize()

            ## Write used tape into database
            database.write_tape_into_database(self.session, next_tape)

            ## Seeking to end of tape, if tape were already used before, check eod with eod from database
            time_started = time.time()
            eod = database.get_end_of_data_by_tape(next_tape)
            if eod is None:
                self.tapelibrary.seek(0)
            else:
                self.tapelibrary.seek(eod)
            logger.debug(f"Execution Time: Seek to end of data: {time.time() - time_started} seconds")

            ## Get free Tapesize
            fs = self.tapelibrary.get_lto4_size_stat()
            logger.info(f"Tape: Used: {fs[0]} ({fs[1]} GB), Free: {fs[2]} ({fs[3]} GB), Total: {fs[4]} ({fs[5]} GB)")

            # If tar is used for writing to file, there will be build 1gb chunks in order to prevent to much storage
            # usage by many small file. Because of the 65kB blocksize it could waste a lot of space.
            files_for_next_chunk = []
            files_next_chunk_size = 0
            files = database.get_files_to_be_written(self.session)
            for file in files:
                free = self.tapelibrary.get_free_tapespace_lto4()
                ## Check if enough space on tape, otherwise unmount and use next tape
                ## The 1gb maximum chunk size doesn't matter here, because were have 10gb buffer, but if the file is
                ## bigger than buffer and bigger than free space, it must be processed as full tape
                if file.encrypted_filesize >= (free - tape_keep_free) or (files_next_chunk_size + file.encrypted_filesize) >= (free - tape_keep_free):
                    if len(files_for_next_chunk) > 0:
                        self.write_file_tar(files_for_next_chunk, free, next_tape)
                        files_for_next_chunk.append(file)
                        files_next_chunk_size += file.encrypted_filesize
                    full = self.tape_is_full_tar(next_tape, free)
                    break
                else:
                    if file.encrypted_filesize >= 1048576:
                        self.write_file_tar([file], free, next_tape)
                    elif (files_next_chunk_size + file.encrypted_filesize) >= 1048576:
                        self.write_file_tar(files_for_next_chunk, free, next_tape)
                        files_for_next_chunk = [file]
                        files_next_chunk_size = file.encrypted_filesize
                    else:
                        files_for_next_chunk.append(file)
                        files_next_chunk_size += file.encrypted_filesize

                if self.interrupted:
                    break

            if len(files_for_next_chunk) > 0:
                self.write_file_tar(files_for_next_chunk, free, next_tape)

        # Info some stats, especially interesting when written is manual interrupted
        logger.info(f"Written {count} of {filecount} files. {self.tools.convert_size(st.f_bavail * st.f_frsize)} "
                    f"space still avalable on tape.")

        if full:
            self.write()

        # Unmounting current tape if interrupted or no more data to write
        self.tapelibrary.unmount()
