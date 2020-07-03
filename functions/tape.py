import logging
import subprocess
import os
import sys
import time
import random
import shutil

logger = logging.getLogger()


class Tape:
    def __init__(self, config, database, tapelibrary, tools, local=False):
        self.config = config
        self.database = database
        self.tapelibrary = tapelibrary
        self.tools = tools
        self.local_files = local
        self.interrupted = False

    def set_interrupted(self):
        self.interrupted = True

    def info(self):
        print("Loaderinfo from Device {}:".format(self.config['devices']['tapelib']))
        for i in self.tapelibrary.loaderinfo():
            print("    {}".format(i.decode('utf-8').rstrip()))

        print("")
        print("Tapeinfo from Device {}:".format(self.config['devices']['tapedrive']))
        for i in self.tapelibrary.tapeinfo():
            print("    {}".format(i.decode('utf-8').rstrip()))

        print("")
        print("MTX Info from Device {}:".format(self.config['devices']['tapelib']))
        for i in self.tapelibrary.mtxinfo():
            print("    {}".format(i.decode('utf-8').rstrip()))

    def status(self):
        tapes, tapes_to_remove = self.tapelibrary.get_tapes_tags_from_library()

        try:
            lto_whitelist = False
            if self.config['lto-whitelist'] is not None:
                lto_whitelist = True
        except KeyError:
            lto_whitelist = False

        if not lto_whitelist:
            print("Ignored Tapes ({}) due to config: {}".format(len(self.config['lto-blacklist']), self.config['lto-blacklist']))
        else:
            print("Whiteliste Tapes ({}) due to config: {}".format(len(self.config['lto-whitelist']), self.config['lto-whitelist']))

        full = []
        for i in self.database.get_full_tapes():
            if lto_whitelist:
                if i[0] not in self.config['lto-whitelist']:
                    continue
            full.append(i[0])
        print("Full tapes: {}".format(full))

        print("")
        print("Free tapes in library({}): {}".format(len(tapes), tapes))

        print("")
        print("Please remove following tapes from library ({}): {}".format(len(tapes_to_remove), tapes_to_remove))

    def filecount_from_verify_files_config(self, filelist):
        if type(self.config['verify-files']) == int:
            return self.config['verify-files']
        else:
            return int(len(filelist) * int(self.config['verify-files'][0:self.config['verify-files'].index("%")]) / 100)

    def test_backup_pieces_ltfs(self, filelist, filecount_to_test):
        logger.info("Testing {} files md5sum".format(filecount_to_test))

        files = self.tools.order_by_startblock(random.choices(filelist, k=filecount_to_test))

        for file in files:
            logger.info("Testing md5sum of file {}".format(file[1]))
            if self.tools.md5sum("{}/{}".format(self.config['local-tape-mount-dir'], file[5])) != file[4]:
                logger.info("md5sum of {} (id: {}) is wrong: exiting!".format(file[1], file[0]))
                return False

            if self.interrupted:
                break
        return True

    def test_backup_pieces_tar(self, filelist, filecount_to_test):
        logger.info("Testing {} files md5sum".format(filecount_to_test))

        files = sorted(random.choices(filelist, k=filecount_to_test), key=lambda i: i[6])

        for file in files:
            logger.info("Testing md5sum of file {}".format(file[1]))

            self.tapelibrary.seek(file[6])
            if self.tools.md5sum_tar(self.config['devices']['tapedrive']) != file[4]:
                logger.info("md5sum of {} (id: {}) is wrong: exiting!".format(file[1], file[0]))
                return False

            if self.interrupted:
                break
        return True

    def write_file_ltfs(self, id, filename, orig_filename, filesize, free, tape):
        logger.debug(f"Tape: Free: {free}, Fileid: {id}, Filesize: {filesize}")

        logger.info(f"Writing file to tape: {orig_filename}")
        time_started = time.time()
        try:
            shutil.copy2(f"{self.config['local-enc-dir']}/{filename}", f"{self.config['local-tape-mount-dir']}/")
        except OSError as error:
            if '[Errno 28]' in str(error):
                logger.error("Tapedevice reports full filesystem. I will revert all files marked as written on this tape and force format this device!")
                logger.error(f"Last reported free size was: {free} ({self.tools.convert_size(free)}), now it shows full!")
                logger.error("This can have different reasons, including a broken drive head. If you are using HPE drives, you can use the tool 'HPE Library and Tape Tools' to check your drive.")
                logger.error("If it happens more often, you should consider to activate the '' option in config.yml")
                logger.error(f"Reverting {len(self.database.get_files_by_tapelabel(tape))} file entries.")
                self.database.revert_written_to_tape_by_label(tape)
                self.tapelibrary.force_mkltfs()
                logger.error("Revert files and force format device finished. Exiting now!")
                sys.exit(1)
            else:
                logger.error(f"Unknown OS Error '{error}', exiting!")
                logger.error(f"You have now stale file entries in database and maybe a broken LTFS, you need to manually format this tape and set written=0, written_date=NULL and tape=NULL on files which has this tape '{tape}' assigned")
        logger.debug(f"Execution Time: Copy file to tape: {time.time() - time_started} seconds")
        self.database.update_file_after_write(int(time.time()), tape, id, None)

    def write_file_tar(self, filelist, free, tape):
        tape_position = self.tapelibrary.get_current_block()
        ids = []
        filesizes = 0
        filenames_enc = []
        for i in filelist:
            ids.append(i[0])
            filesizes += i[3]
            filenames_enc.append(i[1])

        logger.debug("Tape: Free: {}, Fileid: {}, Filesize: {}".format(
            free,
            ids,
            filesizes
        ))

        time_started = time.time()
        count = 0
        for i in filelist:
            count += 1
            logger.info("Writing file to tape ({}/{} in this tar archive): {}".format(count, len(filelist), i[2]))

        # Write file or filelist to tape with tar
        commands = ['tar', '-c', '-b128', '-f', self.config['devices']['tapedrive'], '-C', self.config['local-enc-dir']]
        commands.extend(filenames_enc)
        tar = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setpgrp)
        std_out, std_err = tar.communicate()
        if tar.returncode != 0:
            logger.error('Failed writing tar to tape, manual check is required. Returncode: {}, Error: {}'.format(tar.returncode, std_err))
            sys.exit(1)

        logger.debug("Execution Time: Copy files via tar to tape: {} seconds".format(time.time() - time_started))
        for i in filelist:
            self.database.update_file_after_write(int(time.time()), tape, i[0], tape_position)
        new_tape_position = self.tapelibrary.get_current_block()
        self.database.update_tape_end_position(tape, new_tape_position)

    def tape_is_full_ltfs(self, tape, free):
        # For LTO-5 and above with LTFS support
        logger.warning(
            f"Tape is full ({self.tools.convert_size(free)} left): I am testing now a few media, writing summary into database and unloading tape")

        files = self.database.get_files_by_tapelabel(tape)
        if not self.test_backup_pieces_ltfs(files, self.filecount_from_verify_files_config(files)):
            logger.error(
                "md5sum on tape not equal to database. Stopping everything. Need manual check of the tape!")
            logger.error("If you do not use this tape anymore, or want to write all data again, you need to manual "
                         "modify database. These file IDs were written to tape: {}".format(
                self.database.get_files_by_tapelabel(tape)
            ))
            return False

        self.database.mark_tape_as_full(tape, int(time.time()))

        ## WRITE Database encrypted on tape
        time_started = time.time()
        dt = int(time.time())
        command = ['openssl', 'enc', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in', self.config['database'], '-out',
                   '{}/tapebackup_{}.db.enc'.format(self.config['local-tape-mount-dir'], dt), '-k', self.config['enc-key']]
        openssl = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if len(openssl.stderr.readlines()) > 0:
            logger.error("Writing Database to Tape failed")
            return False

        ## WRITE Textfile containing (encryped_name|original_fullpath) of all files encrypted to tape
        dump = self.database.dump_filenames_to_for_tapes(tape)
        with open('tapebackup_{}.txt'.format(dt), 'w') as f:
            for line in dump:
                f.write('"{}";"{}";"{}"\n'.format(line[0], line[1], line[2]))
        command = ['openssl', 'enc', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in',
                   'tapebackup_{}.txt'.format(dt), '-out',
                   '{}/tapebackup_{}.txt.enc'.format(self.config['local-tape-mount-dir'], dt), '-k', self.config['enc-key']]
        openssl = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if len(openssl.stderr.readlines()) > 0:
            logger.error("Writing Textfile to Tape failed")
            return False
        os.remove('tapebackup_{}.txt'.format(dt))
        logger.debug(
            "Execution Time: Encrypt and write databse to tape: {} seconds".format(time.time() - time_started))

        ## DELETE all Files, that has been transfered to tape
        time_started = time.time()
        for i in self.database.get_files_by_tapelabel(tape):
            if os.path.exists("{}/{}".format(self.config['local-enc-dir'], i[5])):
                logger.info("Deleting encrypted file: {} ({})".format(i[5], i[1]))
                os.remove("{}/{}".format(self.config['local-enc-dir'], i[5]))
        logger.debug("Execution Time: Deleted encrypted files written to tape: {} seconds".format(
            time.time() - time_started))

        ## Unload tape
        self.tapelibrary.unload()
        return True

    def tape_is_full_tar(self, tape, free):
        ## For LTO-4
        logger.warning(
            "Tape is full: I am testing now a few media, writing summary into database and unloading tape")

        files = self.database.get_files_by_tapelabel(tape)
        if not self.test_backup_pieces_tar(files, self.filecount_from_verify_files_config(files)):
            logger.error(
                "md5sum on tape not equal to database. Stopping everything. Need manual check of the tape!")
            logger.error("If you do not use this tape anymore, or want to write all data again, you need to manual "
                         "modify database. These file IDs were written to tape: {}".format(
                self.database.get_files_by_tapelabel(tape)
            ))
            return False

        self.database.mark_tape_as_full(tape, int(time.time()))
        ## TODO: Write DATABASE and stuff to file, see tape_is_full_ltfs

    def write(self):
        full = False
        tapes, tapes_to_remove = self.tapelibrary.get_tapes_tags_from_library()
        if len(tapes_to_remove) > 0:
            logger.warning("These tapes are full, please remove from library: {}".format(tapes_to_remove))

        if len(tapes) == 0:
            logger.error("No free Tapes in Library, but you can remove these full ones: {}".format(tapes_to_remove))
            return

        next_tape = tapes.pop(0)
        logger.info("Using tape {} for writing".format(next_tape))
        self.tapelibrary.load(next_tape)
        lto_version = self.tapelibrary.get_current_lto_version()

        if lto_version >= 5:
            logger.info("LTO-{} Tape found, use LTFS for backup".format(lto_version))
            ## Mount and maybe format tapedevice
            self.tapelibrary.ltfs()

            ## Write used tape into database
            self.database.write_tape_into_database(next_tape)

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
            logger.debug("Execution Time: Getting tape space info: {} seconds".format(time.time() - time_started))

            if "%" in str(self.config['tape-keep-free']):
                tape_keep_free = int(st.f_bavail * st.f_frsize *
                                     int(self.config['tape-keep-free'][0:self.config['tape-keep-free'].index("%")]) /
                                     100)
            else:
                tape_keep_free = self.tools.back_convert_size(str(self.config['tape-keep-free']))
            logger.debug(f"Keep {tape_keep_free} ({self.tools.convert_size(tape_keep_free)}) free on tape given by config file!")

            files = self.database.get_files_to_be_written()
            for file in files:
                st = os.statvfs(self.config['local-tape-mount-dir'])
                free = (st.f_bavail * st.f_frsize)

                ## Check if enough space on tape, otherwise unmount and use next tape
                if file[3] > (free - tape_keep_free - 10737418240):
                    full = self.tape_is_full_ltfs(next_tape, free)
                    break
                else:
                    self.write_file_ltfs(file[0], file[1], file[2], file[3], free, next_tape)

                if self.interrupted:
                    break

        elif lto_version == 4:
            logger.info("LTO-4 Tape found, use tar for backup")
            self.tapelibrary.set_necessary_lto4_options()
            self.tapelibrary.set_blocksize()

            ## Write used tape into database
            self.database.write_tape_into_database(next_tape)

            ## Seeking to end of tape, if tape were already used before, check eod with eod from database
            time_started = time.time()
            eod = self.database.get_end_of_data_by_tape(next_tape)
            if eod is None:
                self.tapelibrary.seek(0)
            else:
                self.tapelibrary.seek(eod)
            logger.debug("Execution Time: Seek to end of data: {} seconds".format(time.time() - time_started))

            ## Get free Tapesize
            fs = self.tapelibrary.get_lto4_size_stat()
            logger.info("Tape: Used: {} ({} GB), Free: {} ({} GB), Total: {} ({} GB)".format(
                fs[0], fs[1], fs[2], fs[3], fs[4], fs[5]
            ))

            # If tar is used for writing to file, there will be build 1gb chunks in order to prevent to much storage
            # usage by many small file. Because of the 65kB blocksize it could waste a lot of space.
            files_for_next_chunk = []
            files_next_chunk_size = 0
            files = self.database.get_files_to_be_written()
            for file in files:
                free = self.tapelibrary.get_free_tapespace_lto4()
                ## Check if enough space on tape, otherwise unmount and use next tape
                ## The 1gb maximum chunk size doesn't matter here, because were have 10gb buffer, but if the file is
                ## bigger than buffer and bigger than free space, it must be processed as full tape
                if file[3] >= (free - 10737418240) or (files_next_chunk_size + file[3]) >= (free - 10737418240):
                    if len(files_for_next_chunk) > 0:
                        self.write_file_tar(files_for_next_chunk, free, next_tape)
                        files_for_next_chunk.append(file)
                        files_next_chunk_size += file[3]
                    full = self.tape_is_full_tar(next_tape)
                    break
                else:
                    if file[3] >= 1048576:
                        self.write_file_tar([file], free, next_tape)
                    elif (files_next_chunk_size + file[3]) >= 1048576:
                        self.write_file_tar(files_for_next_chunk, free, next_tape)
                        files_for_next_chunk = [file]
                        files_next_chunk_size = file[3]
                    else:
                        files_for_next_chunk.append(file)
                        files_next_chunk_size += file[3]

                if self.interrupted:
                    break

            if len(files_for_next_chunk) > 0:
                self.write_file_tar(files_for_next_chunk, free, next_tape)

        if full:
            self.write()
