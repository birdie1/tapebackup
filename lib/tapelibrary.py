import logging
import subprocess
import re
import sys
import os
import time
from typing import List

from lib import database

logger = logging.getLogger()

def send_tape_command(command: list, error_message=None, timeout=30, max_retries=3, sleeptime=1) -> List[str]:
    """
    Send a tape command.

    Because it often returns the error "mtx: Request Sense: Long Report=yes" at the first try, repeat it

    Returnes a list of lines from stdout
    """
    for attempt in range(max_retries):
        try:
            mtx = subprocess.run(command, timeout=timeout, capture_output=True, text=True, check=False)
        except subprocess.TimeoutExpired:
            logger.error("Timeout while trying to reach tapedive, is it running?")
            sys.exit(1)

        error = mtx.stderr.splitlines()
        if len(error) > 0:
            if attempt == max_retries - 1:
                if error_message is not None:
                    logger.error(error_message)
                    logger.debug(error)
                else:
                    logger.error("Giving up reaching tape device. (%s/%s). Error: %s", attempt+1, max_retries, error)
                sys.exit(1)
            logger.warning(
                "Reaching tape device failed, waiting %s seconds for next retry (%s/%s).", sleeptime, attempt+1, max_retries)
            time.sleep(sleeptime)
            continue

        return mtx.stdout.splitlines()
    logger.error("Sending tape command failed! This error should be thrown only when except block did not catch it!")
    sys.exit(1)


class Tapelibrary:
    def __init__(self, config):
        self.config = config
        #self.database = database

    def get_tapes_tags_from_library(self, session):
        time_started = time.time()
        logger.debug("Retrieving current tape tags in library")
        command = ['mtx', '-f', self.config['devices']['tapelib'], 'status']
        mtx_out = send_tape_command(command)
        tag_in_tapelib = []
        tags_to_remove_from_library = []

        try:
            lto_whitelist = len(self.config['lto-whitelist'])
        except TypeError:
            lto_whitelist = None
        except KeyError:
            lto_whitelist = None

        for line in mtx_out:
            if line.find('VolumeTag') != -1:
                tag = line[line.find('=') + 1:].rstrip().lstrip()

                ### If blacklisting is in use
                if not lto_whitelist:
                    if tag in self.config['lto-blacklist']:
                        logger.debug('Ignore Tag {} because exists in ignore list in config'.format(tag))
                    elif database.get_full_tape(session, tag) is not None:
                        logger.debug('Ignore Tag {} because exists in database and is full'.format(tag))
                        tags_to_remove_from_library.append(tag)
                    elif tag.startswith('CLN'):
                        logger.debug(f'Ignore cleaning tape: tag {tag}')
                        pass
                    else:
                        tag_in_tapelib.append(tag)
                else:
                    ### If whitelisting is in use
                    if tag in self.config['lto-whitelist'] and len(database.get_full_tape(session, tag)) > 0:
                        logger.debug('Ignore Tag {} because exists in lto-whitelist, database and is full'.format(tag))
                        tags_to_remove_from_library.append(tag)
                    elif tag in self.config['lto-whitelist']:
                        logger.debug("Tag {} exists in lto whitelist and is ready to use.".format(tag))
                        tag_in_tapelib.append(tag)
                    else:
                        logger.debug('Ignore Tag {} because it is not in lto-whitelist'.format(tag))

        logger.debug("Execution Time: Get tap tags: {} seconds".format(time.time() - time_started))
        logger.debug("Got following tags for usage: {}".format(tag_in_tapelib))
        return tag_in_tapelib, tags_to_remove_from_library

    def get_current_tag_in_transfer_element(self):
        """
        Get the label of the tape which are currently in transfer element
        """
        command = ['mtx', '-f', self.config['devices']['tapelib'], 'status']
        mtx_out = send_tape_command(command)

        for line in mtx_out:
            if 'Data Transfer Element' in line:
                if 'Empty' in line:
                    return False
                return line[line.find('=') + 1:].rstrip().lstrip()
        logger.error("Can't find 'Full' or 'Empty' tag in line 'Data Transfer Element'")

    def get_slot_by_tag(self, tag):
        """
        Get slot in library where the tape is currently located
        """
        command = ['mtx', '-f', self.config['devices']['tapelib'], 'status']
        mtx_out = send_tape_command(command)

        for line in mtx_out:
            if tag in line:
                x = re.search(r".*Storage Element (\d+):Full.*", line)
                return x.group(1)
        return None

    def load_by_tag(self, tag):
        """
        Load a tape by the label from library into drive

        Can take up to 180 seconds
        """
        slot = self.get_slot_by_tag(tag)
        command = ['mtx', '-f', self.config['devices']['tapelib'], 'load', slot]
        send_tape_command(command, error_message="Cant load tape into drive, giving up", timeout=180)
        logger.info("Tape {} loaded successfully".format(tag))

    def unmount(self):
        """
        Unmounting LTFS from local tape mount dir
        """
        time_started = time.time()
        logger.debug("Unmounting: %s", self.config['local-tape-mount-dir'])
        command = ['umount', self.config['local-tape-mount-dir']]
        send_tape_command(command, error_message="Cant unmount tape, giving up")
        logger.debug("Execution Time: Unmounting tape: %s seconds", time.time() - time_started)

    def unload(self):
        """
        Unloading tape from drive back into library

        Can take up to 180 seconds.
        """
        if os.path.ismount(self.config['local-tape-mount-dir']):
            self.unmount()

        time_started = time.time()
        command = ['mtx', '-f', self.config['devices']['tapelib'], 'unload']
        send_tape_command(command, error_message="Cant unload tape from drive into library, giving up", timeout=180)

        logger.info("Drive unloaded loaded successfully")
        logger.debug("Execution Time: Unloading tape: %s seconds", time.time() - time_started)

    def load(self, next_tape):
        """
        Load a tape into drive
        """
        time_started = time.time()
        loaded_tag = self.get_current_tag_in_transfer_element()

        if not loaded_tag:
            logger.info("Loading tape (%s) into drive", next_tape)
            self.load_by_tag(next_tape)
        else:
            if loaded_tag != next_tape:
                logger.info("Wrong tape in drive: unloading!")

                self.unload()

                logger.debug("Drive unloaded")
                logger.info("Loading tape (%s) into drive", next_tape)

                self.load_by_tag(next_tape)
        logger.debug("Execution Time: Load tape into tapedrive: %s seconds",  time.time() - time_started)

    def ltfs(self):
        """
        Try to mount ltfs, if not possible, make ltfs filesystem and then mount
        """
        mounted = self.mount_ltfs()
        if not mounted:
            self.mkltfs()
            self.mount_ltfs()

    def mkltfs(self):
        """
        Make ltfs on tape
        """
        time_started = time.time()
        commands = ['mkltfs', '-d', self.config['devices']['tapedrive'], '--no-compression']
        ltfs = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = ltfs.communicate()

        logger.info("Formating Tape: %s", std_out)
        if ltfs.returncode != 0:
            logger.debug("Return code: %s", ltfs.returncode)
            logger.debug("std out: %s", std_out)
            logger.debug("std err: %s", std_err)
        logger.debug("Execution Time: Make LTFS: %s seconds", time.time() - time_started)

    def force_mkltfs(self):
        # Caution! This will force overriding existing tape. Use it only in case of 'No Space left on device' problems!
        if os.path.ismount(self.config['local-tape-mount-dir']):
            self.unmount()

        # Add sleep to prevent tapedrive from being locked
        time.sleep(60)

        time_started = time.time()
        commands = ['mkltfs', '-f', '-d', self.config['devices']['tapedrive'], '--no-compression']
        ltfs = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = ltfs.communicate()

        if ltfs.returncode != 0:
            logger.debug(f"Return code: {ltfs.returncode}")
            logger.debug(f"std out: {std_out}")
            logger.debug(f"std err: {std_err}")
            logger.error("Formatting tape failed, you need to manually format this tape before next usage!")
        logger.debug("Execution Time: Force making LTFS: {} seconds".format(time.time() - time_started))

    def mount_ltfs(self):
        """
        Mount ltfs on tape to a local directory

        Can tape up to 60 seconds
        """
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
                logger.error("Tapedrive mountpoint not found, please create folder: %s", self.config['local-tape-mount-dir'])
                sys.exit(1)

            logger.error("Unknown error when trying to mount")
            logger.debug("Execution Time: Mount LTFS: %s seconds", time.time() - time_started)
            sys.exit(1)

        else:
            logger.info("LTFS successfully mounted")
            logger.debug("Execution Time: Mount LTFS: %s seconds", time.time() - time_started)
            return True

    def loaderinfo(self):
        """
        Get loadinfo
        """
        command = ['loaderinfo', '-f', self.config['devices']['tapelib']]
        return send_tape_command(command)


    def tapeinfo(self):
        """
        Get tapeinfo
        """
        command = ['tapeinfo', '-f', self.config['devices']['tapedrive']]
        return send_tape_command(command)

    def mtxinfo(self):
        """
        Get mtx info
        """
        command = ['mtx', '-f', self.config['devices']['tapelib'], 'status']
        return send_tape_command(command)

    def get_current_lto_version(self):
        """
        Get current lto version in transfer element
        """
        loaded_tag = self.get_current_tag_in_transfer_element()
        x = re.search(r".*L(\d)$", loaded_tag)
        return int(x.group(1))

    def get_current_blocksize(self):
        command = ['mt-st', '-f', self.config['devices']['tapedrive'], 'status']
        mtx_out = send_tape_command(command)

        for line in mtx_out:
            if 'Tape block size' in line:
                x = re.search(r".*Tape block size (\d*) bytes.*", line)
                return int(x.group(1))
        return False

    def set_necessary_lto4_options(self):
        commands = ['mt-st', '-f', self.config['devices']['tapedrive'], 'stsetoptions', 'scsi2logical']
        mt_st = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = mt_st.communicate()

        if mt_st.returncode != 0:
            logger.error("Setting LTO4 options failed")
            return False
        else:
            return True

    def set_blocksize(self):
        commands = ['mt-st', '-f', self.config['devices']['tapedrive'], 'setblk', '64k']
        mt_st = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = mt_st.communicate()

        if mt_st.returncode == 0 and self.get_current_blocksize() == 65536:
            return True
        else:
            logger.error("Setting block size failed")
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

    def seek(self, position):
        logger.info("Seeking tape to position {}".format(position))
        time_started = time.time()
        commands = ['mt-st', '-f', self.config['devices']['tapedrive'], 'seek', str(position)]
        mt_st = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std_out, std_err = mt_st.communicate()
        logger.debug(
            "Execution Time: Seeking tape to position {}: {} seconds".format(position, time.time() - time_started))

        if mt_st.returncode == 0:
            if self.get_current_block() == position:
                logger.debug("Tape is on position {}".format(self.get_current_block()))
                return True
            else:
                logger.error("Tape is on position {}, expected {}".format(self.get_current_block(), position))
                sys.exit(1)
        else:
            logger.error("Executing 'mt-st -f /dev/nst0 seek {}' failed".format(position))
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
