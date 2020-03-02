import logging
import subprocess
import os
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


    def test_backup_pieces(self, filelist, percent):
        filecount_to_test = int(len(filelist) * percent / 100)
        logger.info("Testing {} files md5sum".format(filecount_to_test))
        for i in range(filecount_to_test):
            index = random.randrange(0, len(filelist))
            logger.info("Testing md5sum of file {}".format(filelist[index][3]))
            if self.tools.md5sum("{}/{}".format(self.config['local-tape-mount-dir'], filelist[index][1])) != filelist[index][2]:
                return False
        return True


    def write_file(self, id, filename, md5, orig_filename, filesize, tape):
        ##Get free tapesize, filesize and compare with a space blocker of 1GB and test 5% of the written media
        st = os.statvfs(self.config['local-tape-mount-dir'])
        free = (st.f_bavail * st.f_frsize)

        logger.debug("Tape: Free: {}, Used: {}, Fileid: {}, Filesize: {}".format(
            free,
            int((st.f_blocks - st.f_bfree) * st.f_frsize),
            id,
            filesize
        ))

        logger.info("Writing file to tape: {}".format(orig_filename))
        time_started = time.time()
        shutil.copy2("{}/{}".format(self.config['local-enc-dir'], filename), "{}/".format(self.config['local-tape-mount-dir']))
        logger.debug("Execution Time: Copy file to tape: {} seconds".format(time.time() - time_started))
        self.database.update_file_after_write(int(time.time()), tape, id)


    def full_tape(self, tape):
        logger.warning(
            "Tape is full: I am testing now a few media, writing summary into database and unloading tape")

        if not self.test_backup_pieces(self.database.get_files_by_tapelabel(tape), 5):
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
        for i in self.database.get_files_by_tapelabel(next_tape):
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

        ## Load tape, mount and maybe format tapedevice
        self.tapelibrary.load(next_tape)
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
            filesize = os.path.getsize("{}/{}".format(self.config['local-enc-dir'], file[1]))

            ## Check if enough space on tape, otherwise unmount and use next tape
            if filesize > ((st.f_bavail * st.f_frsize) - 10737418240):
                full = self.full_tape(next_tape)
                break
            else:
                self.write_file(file[0], file[1], file[2], file[3], filesize, next_tape)

            if self.interrupted:
                break

        if full:
            self.write()
