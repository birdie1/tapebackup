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


    def test_backup_pieces(self, filelist, verify_files):
        if type(verify_files) == int:
            filecount_to_test = verify_files
        else:
            filecount_to_test = int(len(filelist) * int(verify_files[0:verify_files.index("%")]) / 100)

        logger.info("Testing {} files md5sum".format(filecount_to_test))
        for i in range(filecount_to_test):
            index = random.randrange(0, len(filelist))
            logger.info("Testing md5sum of file {}".format(filelist[index][3]))
            if self.tools.md5sum("{}/{}".format(self.config['local-tape-mount-dir'], filelist[index][1])) != filelist[index][2]:
                return False

            if self.interrupted:
                break
        return True


    def write_file_ltfs(self, id, filename, orig_filename, filesize, free, tape):
        logger.debug("Tape: Free: {}, Fileid: {}, Filesize: {}".format(
            free,
            id,
            filesize
        ))

        logger.info("Writing file to tape: {}".format(orig_filename))
        time_started = time.time()
        shutil.copy2("{}/{}".format(self.config['local-enc-dir'], filename), "{}/".format(self.config['local-tape-mount-dir']))
        logger.debug("Execution Time: Copy file to tape: {} seconds".format(time.time() - time_started))
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
        for i in filelist:
            logger.info("Writing file to tape: {}".format(i[2]))

        # Write file or filelist to tape with tar
        commands = ['tar', '-c', '-b128', '-f', self.config['devices']['tapedrive'], '-C', self.config['local-enc-dir']]
        commands.extend(filenames_enc)
        tar = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = tar.communicate()
        if tar.returncode != 0:
            logger.error('Failed writing tar to tape, manual check is required')
            sys.exit(1)

        logger.debug("Execution Time: Copy files via tar to tape: {} seconds".format(time.time() - time_started))
        for i in filelist:
            self.database.update_file_after_write(int(time.time()), tape, i[0], tape_position)
        new_tape_position = self.tapelibrary.get_current_block()
        self.database.update_tape_end_position(tape, new_tape_position)

    def full_tape(self, tape):
        logger.warning(
            "Tape is full: I am testing now a few media, writing summary into database and unloading tape")

        if not self.test_backup_pieces(self.database.get_files_by_tapelabel(tape), self.config['verify-files']):
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
            if os.path.exists("{}/{}".format(self.config['local-enc-dir'], i[1])):
                logger.info("Deleting encrypted file: {} ({})".format(i[3], i[1]))
                os.remove("{}/{}".format(self.config['local-enc-dir'], i[1]))
        logger.debug("Execution Time: Deleted encrypted files written to tape: {} seconds".format(
            time.time() - time_started))

        ## Unload tape
        self.tapelibrary.unload()
        return True

    def write(self):
        full = False
        tapes, tapes_to_remove = self.tapelibrary.get_tapes_tags_from_library()
        if len(tapes_to_remove) > 0:
            logger.warning("These tapes are full, please remove from library: {}".format(tapes_to_remove))

        if len(tapes) == 0:
            logger.error("No free Tapes in Library, but you can remove these full once: {}".format(tapes_to_remove))
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

            files = self.database.get_files_to_be_written()
            for file in files:
                st = os.statvfs(self.config['local-tape-mount-dir'])
                free = (st.f_bavail * st.f_frsize)

                ## Check if enough space on tape, otherwise unmount and use next tape
                if file[3] > (free - 10737418240):
                    full = self.full_tape(next_tape)
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

            ## Seeking to end of tape, if tape were already
            time_started = time.time()
            eod = self.database.get_end_of_data_by_tape(next_tape)
            if eod is None:
                self.tapelibrary.seek_to_end_of_data(0)
            else:
                self.tapelibrary.seek_to_end_of_data(eod)
            logger.debug("Execution Time: Seek to end of data: {} seconds".format(time.time() - time_started))

            ## Get free Tapesize
            fs = self.tapelibrary.get_lto4_size_stat()
            logger.info("Tape: Used: {} ({} GB), Free: {} ({} GB), Total: {} ({} GB)".format(
                fs[0],
                fs[1],
                fs[2],
                fs[3],
                fs[4],
                fs[5]
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
                if file[3] >= (free - 10737418240) or (files_for_next_chunk + file[3]) >= (free - 10737418240):
                    if len(files_for_next_chunk) > 0:
                        self.write_file_tar(files_for_next_chunk, free, next_tape)
                        files_for_next_chunk.append(file)
                        files_next_chunk_size += file[3]
                    # TODO: Special full function for lto-4
                    #full = self.full_tape(next_tape)
                    #break
                    pass
                else:
                    if file[3] >= 1048576:
                        self.write_file_tar([file], free, next_tape)
                    elif (files_for_next_chunk + file[3]) >= 1048576:
                        self.write_file_tar(files_for_next_chunk, free, next_tape)
                        files_for_next_chunk = []
                        files_next_chunk_size = 0
                    else:
                        files_for_next_chunk.append(file)
                        files_next_chunk_size += file[3]

                if self.interrupted:
                    break

            if len(files_for_next_chunk) > 0:
                self.write_file_tar(files_for_next_chunk, free, next_tape)

        if full:
            self.write()
