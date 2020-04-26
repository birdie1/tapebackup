import logging
import subprocess
import re
import sys
import os
import time

logger = logging.getLogger()


class Tapelibrary:
    def __init__(self, config, database):
        self.config = config
        self.database = database

    def get_tapes_tags_from_library(self):
        time_started = time.time()
        logger.debug("Retrieving current tape tags in library")
        commands = ['mtx', '-f', self.config['devices']['tapelib'], 'status']
        mtx = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        tag_in_tapelib = []
        tags_to_remove_from_library = []

        try:
            lto_whitelist = len(self.config['lto-whitelist'])
        except TypeError:
            lto_whitelist = None
        except KeyError:
            lto_whitelist = None

        for i in mtx.stdout.readlines():
            line = i.decode('utf-8').rstrip().lstrip()
            if line.find('VolumeTag') != -1:
                tag = line[line.find('=') + 1:].rstrip().lstrip()

                ### If blacklisting is in use
                if lto_whitelist == 0 or lto_whitelist is None:
                    if tag in self.config['lto-blacklist']:
                        logger.debug('Ignore Tag {} because exists in ignore list in config'.format(tag))
                    elif len(self.database.get_full_tape(tag)) > 0:
                        logger.debug('Ignore Tag {} because exists in database and is full'.format(tag))
                        tags_to_remove_from_library.append(tag)
                    else:
                        tag_in_tapelib.append(tag)
                else:
                    ### If whitelisting is in use
                    if tag in self.config['lto-whitelist'] and len(self.database.get_full_tape(tag)) > 0:
                        logger.debug('Ignore Tag {} because exists in lto-whitelist, database and is full'.format(tag))
                        tags_to_remove_from_library.append(tag)
                    elif tag in self.config['lto-whitelist']:
                        logger.debug("Tag {} exists in lto whitelist and is ready to use.".format(tag))
                        tag_in_tapelib.append(tag)
                    else:
                        logger.debug('Ignore Tag {} because it is not in lto-whitelist'.format(tag))

        logger.debug("Execution Time: Encrypt file with openssl: {} seconds".format(time.time() - time_started))
        logger.debug("Got following tags for usage: {}".format(tag_in_tapelib))
        return tag_in_tapelib, tags_to_remove_from_library

    def get_current_tag_in_transfer_element(self):
        commands = ['mtx', '-f', self.config['devices']['tapelib'], 'status']
        mtx = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        for i in mtx.stdout.readlines():
            line = i.decode('utf-8').rstrip().lstrip()
            if 'Data Transfer Element' in line:
                if 'Empty' in line:
                    return False
                elif 'Full' in line:
                    return line[line.find('=') + 1:].rstrip().lstrip()
        logger.error("Can't find 'Full' or 'Empty' tag in line 'Data Transfer Element'")

    def get_slot_by_tag(self, tag):
        commands = ['mtx', '-f', self.config['devices']['tapelib'], 'status']
        mtx = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        for i in mtx.stdout.readlines():
            line = i.decode('utf-8').rstrip().lstrip()
            if tag in line:
                x = re.search(r".*Storage Element (\d+):Full.*", line)
                return x.group(1)

    def load_by_tag(self, tag):
        slot = self.get_slot_by_tag(tag)
        commands = ['mtx', '-f', self.config['devices']['tapelib'], 'load', slot]
        mtx = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if len(mtx.stderr.readlines()) > 0:
            logger.error("Cant load tape into drive, giving up")
            sys.exit(1)
        else:
            logger.info("Tape {} loaded successfully".format(tag))


    def unload(self):
        time_started = time.time()
        if os.path.ismount(self.config['local-tape-mount-dir']):
            logger.debug("Unmounting: {}".format(self.config['local-tape-mount-dir']))
            commands = ['umount', self.config['local-tape-mount-dir']]
            umount = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if len(umount.stderr.readlines()) > 0:
                logger.error("Cant unmount, giving up")
                sys.exit(1)

        commands = ['mtx', '-f', self.config['devices']['tapelib'], 'unload']
        mtx = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if len(mtx.stderr.readlines()) > 0:
            logger.error("Cant unload drive, giving up")
            logger.debug("Execution Time: Unloading tape: {} seconds".format(time.time() - time_started))
            sys.exit(1)
        else:
            logger.info("Drive unloaded loaded successfully")

        logger.debug("Execution Time: Unloading tape: {} seconds".format(time.time() - time_started))

    def load(self, next_tape):
        time_started = time.time()
        loaded_tag = self.get_current_tag_in_transfer_element()

        if not loaded_tag:
            logger.info("Loading tape ({}) into drive".format(next_tape))
            self.load_by_tag(next_tape)
        else:
            if loaded_tag != next_tape:
                logger.info("Wrong tape in drive: unloading!")

                self.unload()

                logger.debug("Drive unloaded")
                logger.info("Loading tape ({}) into drive".format(next_tape))

                self.load_by_tag(next_tape)
        logger.debug("Execution Time: Load tape into tapedrive: {} seconds".format(time.time() - time_started))

    def ltfs(self):
        mounted = self.mount_ltfs()
        if not mounted:
            self.mkltfs()
            self.mount_ltfs()

    def mkltfs(self):
        time_started = time.time()
        commands = ['mkltfs', '-d', self.config['devices']['tapedrive']]
        ltfs = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = ltfs.communicate()

        logger.info("Formating Tape: {}".format(std_out))
        logger.debug("Execution Time: Make LTFS Filesystem: {} seconds".format(time.time() - time_started))

    def mount_ltfs(self):
        time_started = time.time()
        if os.path.ismount(self.config['local-tape-mount-dir']):
            logger.debug('LTFS already mounted, skip mounting')
            return True

        commands = [ 'ltfs', self.config['local-tape-mount-dir'] ]
        ltfs = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = ltfs.communicate()

        if ltfs.returncode != 0:
            error = std_err.decode('utf-8')

            if 'Cannot read volume: medium is not partitioned' in error:
                logger.warning("Current tape needs mkltfs before mounting is possible. Making filesystem.")
                return False

            x = re.findall("Mountpoint .*{}.* specified but not accessible".format(self.config['local-tape-mount-dir']), error)
            if len(x) > 0:
                logger.error("Tapedrive mountpoint not found, please create folder: {}".format(self.config['local-tape-mount-dir']))
                sys.exit(1)

            logger.error("Unknown error when trying to mount")
            logger.debug("Execution Time: Mount LTFS: {} seconds".format(time.time() - time_started))
            sys.exit(1)

        else:
            logger.info("LTFS successfully mounted")
            logger.debug("Execution Time: Mount LTFS: {} seconds".format(time.time() - time_started))
            return True

    def loaderinfo(self):
        commands = ['loaderinfo', '-f', self.config['devices']['tapelib']]
        loaderinfo = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        return loaderinfo.stdout.readlines()

    def tapeinfo(self):
        commands = ['tapeinfo', '-f', self.config['devices']['tapedrive']]
        tapeinfo = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        return tapeinfo.stdout.readlines()

    def mtxinfo(self):
        commands = ['mtx', '-f', self.config['devices']['tapelib'], 'status']
        mtx = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        return mtx.stdout.readlines()

    def get_current_lto_version(self):
        loaded_tag = self.get_current_tag_in_transfer_element()
        x = re.search(r".*L(\d)$", loaded_tag)
        return int(x.group(1))

    def get_current_blocksize(self):
        commands = ['mt-st', '-f', self.config['devices']['tapedrive'], 'status']
        mt_st = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        for i in mt_st.stdout.readlines():
            line = i.decode('utf-8').rstrip().lstrip()
            if 'Tape block size' in line:
                x = re.search(r".*Tape block size (\d*) bytes.*", line)
                return int(x.group(1))
        return False

    def set_necessary_lto4_options(self):
        commands = ['mt-st', '-f', self.config['devices']['tapedrive'], 'stsetoptions', 'scsi2logical']
        mt_st = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = mt_st.communicate()

        if mt_st.returncode != 0:
            logger.error("Executing 'mt-st -f /dev/nst0 setblk 64k' failed")
            return False
        else:
            return True

    def set_blocksize(self):
        commands = ['mt-st', '-f', self.config['devices']['tapedrive'], 'setblk', '64k']
        mt_st = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = mt_st.communicate()

        if mt_st.returncode == 0:
            if self.get_current_blocksize() == 65536:
                return True
            else:
                logger.error('Set tape blocksize to 65536 failed')
        else:
            logger.error("Executing 'mt-st -f /dev/nst0 setblk 64k' failed")
        return False

    def get_current_block(self):
        commands = ['mt-st', '-f', self.config['devices']['tapedrive'], 'tell']
        mt_st = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        for i in mt_st.stdout.readlines():
            line = i.decode('utf-8').rstrip().lstrip()
            if 'At block' in line:
                x = re.search(r"At block (\d*).", line)
                return int(x.group(1))
        return False

    def get_max_block(self):
        commands = ['tapeinfo', '-f', self.config['devices']['tapedrive']]
        mt_st = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        for i in mt_st.stdout.readlines():
            line = i.decode('utf-8').rstrip().lstrip()
            if 'MaxBlock' in line:
                x = re.search(r"MaxBlock: (\d*)", line)
                return int(x.group(1))
        return False

    def seek_to_end_of_data(self, expected_end):
        commands = ['mt-st', '-f', self.config['devices']['tapedrive'], 'eod']
        mt_st = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = mt_st.communicate()

        if mt_st.returncode == 0:
            if self.get_current_block() == expected_end:
                logger.debug("Tape is on position {}".format(self.get_current_block()))
                return True
            else:
                logger.error("Tape is on position {}, expected {}".format(self.get_current_block(), expected_end))
                sys.exit(1)
        else:
            logger.error("Executing 'mt-st -f /dev/nst0 eod' failed")
            sys.exit(1)

    def get_lto4_size_stat(self):
        data = []
        max_block = self.get_max_block()
        current_block = self.get_current_block()
        block_size = self.get_current_blocksize()

        data.append((current_block - 1) * block_size)
        data.append(int((current_block - 1) * block_size / 1024 / 1024 / 1024))
        data.append((max_block - current_block) * block_size)
        data.append(int((max_block - current_block) * block_size / 1024 / 1024 / 1024))
        data.append(max_block * block_size)
        data.append(int(max_block * block_size / 1024 / 1024 / 1024))

        return data

    def get_free_tapespace_lto4(self):
        max_block = self.get_max_block()
        current_block = self.get_current_block()
        block_size = self.get_current_blocksize()
        return (max_block - current_block) * block_size
