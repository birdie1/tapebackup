#!/usr/bin/env python3

import yaml
import sys
import logging
import argparse
import os
import time
import signal
import subprocess
import secrets
import string
import random
import shutil
from lib import Database, Tapelibrary, Tools

pname = "Tapebackup"
pversion = '0.1.1'
debug = False

logging.basicConfig(level=logging.DEBUG,
                    format='[%(levelname)-7s] (%(asctime)s) %(filename)s::%(lineno)d %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='main.log')
logger = logging.getLogger()

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('[%(levelname)-7s] (%(asctime)s) %(filename)s::%(lineno)d %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)



def signal_handler(signal, frame):
    global interrupted
    global child_process_pid
    if interrupted:
        print('Pressed CTRL + C twice, giving up. Please check the database for broken entry!')
        os.kill(child_process_pid, signal)
    else:
        interrupted = True
        print('\n I will stop after current Operation!')

signal.signal(signal.SIGINT, signal_handler)
interrupted = False
child_process_pid = 0


def test_backup_pieces(filelist, percent):
    filecount_to_test = int(len(filelist) * percent / 100)
    logger.info("Testing {} files md5sum".format(filecount_to_test))
    for i in range(filecount_to_test):
        index = random.randrange(0, len(filelist))
        logger.info("Testing md5sum of file {}".format(filelist[index][3]))
        if tools.md5sum("{}/{}".format(cfg['local-tape-mount-dir'], filelist[index][1])) != filelist[index][2]:
            return False

    return True


########## main functions from here ##########
def show_version():
    print("{}: Version {}".format(pname, pversion))

def config_override_from_cmd():
    if args.database is not None:
        cfg['database'] = args.database
    if args.data_dir is not None:
        cfg['local-data-dir'] = args.data_dir
    if args.server is not None:
        cfg['remote-server'] = args.server
    if args.tape_mount is not None:
        cfg['local-tape-mount-dir'] = args.tape_mount
    if args.tapedrive is not None:
        cfg['devices']['tapedrive'] = args.tapedrive
    if args.tapelib is not None:
        cfg['devices']['tapelib'] = args.tapelib


def print_debug_info():
    print("Command: {}".format(args.command))
    print("")
    print("CONFIG ARGUMENTS")
    print("Tapelib: {}".format(cfg['devices']['tapelib']))
    print("Tapedrive: {}".format(cfg['devices']['tapedrive']))
    print("Database: {}".format(cfg['database']))
    print("Database Backup Path: {}".format(cfg['database-backup-git-path']))
    print("Remote Server: {}".format(cfg['remote-server']))
    print("Remote Base Directory: {}".format(cfg['remote-base-dir']))
    print("Remote Data Directory: {}".format(cfg['remote-data-dir']))
    print("Local Data Directory: {}".format(cfg['local-data-dir']))
    print("Local Encryption Directory: {}".format(cfg['local-enc-dir']))
    print("Local Tape Mount Directory: {}".format(cfg['local-tape-mount-dir']))
    print("Encryption Key: {}".format(cfg['enc-key']))
    i = 0
    for j in cfg['lto-ignore-tapes']:
        print("Ignored Tape {}, Label: {}".format(i, j))
        i += 1
    print("")
    print("CMD ARGUMENTS")
    print("--config: {}".format(args.config))
    print("--data-dir: {}".format(args.data_dir))
    print("--database: {}".format(args.database))
    print("--debug: {}".format(args.debug))
    print("--info: {}".format(args.info))
    print("--quiet: {}".format(args.quiet))
    print("--server: {}".format(args.server))
    print("--tape-mount: {}".format(args.tape_mount))
    print("--tapedrive: {}".format(args.tapedrive))
    print("--tapelib: {}".format(args.tapelib))
    print("--version: {}".format(args.version))


def create_key():
    alphabet = string.ascii_letters + string.digits
    print(''.join(secrets.choice(alphabet) for i in range(128)))


def init_db():
    if database.create_tables():
        logger.info("Tables created")


def repair_db():
    broken_d = database.get_broken_db_download_entry()
    for file in broken_d:
        if os.path.isfile("{}/{}".format(cfg['local-data-dir'], file[1])):
            os.remove("{}/{}".format(cfg['local-data-dir'], file[1]))

        logger.info("Fixing Database ID: {}".format(file[0]))
        database.delete_broken_db_download_entry(file[0])

    logger.info("Fixed {} messed up download entries".format(len(broken_d)))


    broken_p = database.get_broken_db_encrypt_entry()
    for file in broken_p:
        if os.path.isfile("{}/{}".format(cfg['local-enc-dir'], file[1])):
            os.remove("{}/{}".format(cfg['local-enc-dir'], file[1]))

        logger.info("Fixing Database ID: {}".format(file[0]))
        database.update_broken_db_encrypt_entry(file[0])

    logger.info("Fixed {} messed up encrypt entries".format(len(broken_p)))


def status_db():
    tables = database.get_tables()

    for i in tables:
        print("")
        print("######### SHOW TABLE {} ##########".format(i))
        database.total_rows(i, print_out=True)
        database.table_col_info(i, print_out=True)
        database.values_in_col(i, print_out=True)


def backup_db():
    database.export('{}/tapebackup-{}.sql'.format(cfg['database-backup-git-path'], int(time.time())))
    ## TODO: Compare to old git and commit if changed


def get_files():
    global interrupted
    global child_process_pid

    if args.local:
        logger.info("Retrieving file list from server LOCAL directory '{}'".format(os.path.abspath(cfg['local-data-dir'])))
        result = tools.ls_recursive(os.path.abspath(cfg['local-data-dir']))
    else:
        logger.info("Retrieving file list from server '{}' directory '{}'".format(cfg['remote-server'], cfg['remote-data-dir']))
        commands = ['ssh', cfg['remote-server'], 'find "{}" -type f'.format(cfg['remote-data-dir'])]
        ssh = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        result = ssh.stdout.readlines()
        logger.info("Got file list from server {} directory '{}'".format(cfg['remote-server'], cfg['remote-data-dir']))

    file_count_total = len(result)
    logger.info("Found {} entries. Start to process.".format(file_count_total))

    downloaded_count = 0
    skipped_count = 0
    failed_count = 0

    for fpath in result:
        if isinstance(fpath, bytes):
            fullpath = fpath.decode("UTF-8").rstrip()
            relpath = tools.strip_base_path(fullpath, cfg['remote-base-dir'])
        else:
            fullpath = fpath.rstrip()
            relpath = tools.strip_base_path(fullpath, cfg['local-base-dir'])
        logger.debug("Processing {}".format(fullpath))

        filename = tools.strip_path(fullpath)
        dir = tools.strip_filename(relpath)

        if not database.check_if_file_exists_by_path(relpath):
            logger.info("Processing {}".format(fullpath))
            id = database.insert_file(filename, relpath)
            logger.debug("Inserting file into database. Fileid: {}".format(id))
            downloaded = False

            if not args.local:
                os.makedirs("{}/{}".format(cfg['local-data-dir'], dir), exist_ok=True)

                command = ['rsync', '--protect-args', '-ae', 'ssh', '{}:{}'.format(cfg['remote-server'], fullpath), '{}/{}'.format(cfg['local-data-dir'], dir)]
                rsync = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setpgrp)
                child_process_pid = rsync.pid

                if len(rsync.stderr.readlines()) == 0:
                    downloaded = True

            if args.local or downloaded:
                if args.local:
                    mtime = int(os.path.getmtime(os.path.abspath("{}/{}".format(cfg['local-base-dir'], relpath))))
                    md5 = tools.md5sum(os.path.abspath("{}/{}".format(cfg['local-base-dir'], relpath)))
                else:
                    mtime = int(os.path.getmtime(os.path.abspath("{}/{}".format(cfg['local-data-dir'], relpath))))
                    md5 = tools.md5sum(os.path.abspath("{}/{}".format(cfg['local-data-dir'], relpath)))
                downloaded_date = int(time.time())

                duplicate = database.get_files_by_md5(md5)
                if len(duplicate) > 0:
                    logger.info("File downloaded with another name. Storing filename in Database: {}".format(filename))
                    duplicate_id = duplicate[0][0]
                    inserted_id = database.insert_alternative_file_names(filename, relpath, duplicate_id, downloaded_date)
                    database.delete_broken_db_download_entry(id)
                    if not args-local:
                        os.remove(os.path.abspath("{}/{}".format(cfg['local-data-dir'], relpath)))
                    skipped_count += 1
                else:
                    database.update_file_after_download(mtime, downloaded_date, md5, 1, id)
                    downloaded_count += 1
                    logger.debug("Download finished: {}".format(relpath))
            else:
                logger.warning("Download failed, file: {} error: {}".format(relpath, rsync.stderr.readlines()))
                failed_count += 1
        else:
            logger.debug("File already downloaded, skipping {}".format(relpath))
            skipped_count += 1

        if interrupted:
            break

    logger.info("Processing finished: downloaded: {}, skipped (already downloaded): {}, failed: {}".format(downloaded_count, skipped_count, failed_count))


def encrypt_files():
    global interrupted
    global child_process_pid

    logger.info("Starting encrypt files job")

    files = database.get_files_to_be_encrypted()
    alphabet = string.ascii_letters + string.digits

    for file in files:
        id = file[0]
        filepath = file[2]

        logger.info("Processing: id: {}, filename: {}".format(id, file[1]))

        filename_enc_helper = ''.join(secrets.choice(alphabet) for i in range(64))
        filename_enc = "{}.enc".format(filename_enc_helper)

        database.update_filename_enc(filename_enc, id)

        if not args.local:
            command = ['openssl', 'enc', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in', os.path.abspath('{}/{}'.format(cfg['local-data-dir'], filepath)), '-out', os.path.abspath('{}/{}'.format(cfg['local-enc-dir'], filename_enc)), '-k', cfg['enc-key']]
        else:
            command = ['openssl', 'enc', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in', os.path.abspath('{}/{}'.format(cfg['local-base-dir'], filepath)), '-out', os.path.abspath('{}/{}'.format(cfg['local-enc-dir'], filename_enc)), '-k', cfg['enc-key']]
        openssl = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setpgrp)
        child_process_pid = openssl.pid

        if len(openssl.stderr.readlines()) == 0:
            md5 = tools.md5sum(os.path.abspath("{}/{}".format(cfg['local-enc-dir'], filename_enc)))
            encrypted_date = int(time.time())
            database.update_file_after_encrypt(encrypted_date, md5, id)

            if not args.local:
                os.remove(os.path.abspath("{}/{}".format(cfg['local-data-dir'], filepath)))
        else:
            logger.warning("encrypt file failed, file: {} error: {}".format(id, openssl.stderr.readlines()))

        if interrupted:
            break

    ## encrypt
    # openssl enc -aes-256-cbc -pbkdf2 -iter 100000 -in 'videofile.mp4' -out test.enc -k supersicherespasswort
    ## decrypt
    # openssl enc -d -aes-256-cbc -pbkdf2 -iter 100000 -in test.enc -out test.mp4


def tapeinfo():
    print("Loaderinfo from Device {}:".format(cfg['devices']['tapelib']))
    for i in tapelibrary.loaderinfo():
        print("    {}".format(i.decode('utf-8').rstrip()))

    print("")
    print("Tapeinfo from Device {}:".format(cfg['devices']['tapedrive']))
    for i in tapelibrary.tapeinfo():
        print("    {}".format(i.decode('utf-8').rstrip()))

    print("")
    print("MTX Info from Device {}:".format(cfg['devices']['tapelib']))
    for i in tapelibrary.mtxinfo():
        print("    {}".format(i.decode('utf-8').rstrip()))

def tapestatus():
    tapes, tapes_to_remove = tapelibrary.get_tapes_tags_from_library()

    print("")
    print("Ignored Tapes ({}) due to config: {}".format(len(cfg['lto-ignore-tapes']), cfg['lto-ignore-tapes']))
    print("Full tapes: {}".format(database.get_full_tapes()))

    print("")
    print("Free tapes in library({}): {}".format(len(tapes), tapes))

    print("")
    print("Please remove following tapes from library ({}): {}".format(len(tapes_to_remove), tapes_to_remove))


def write_files():
    recursive = False
    tapes, tapes_to_remove = tapelibrary.get_tapes_tags_from_library()
    if len(tapes_to_remove) > 0:
        logger.warning("These tapes are full, please remove from library: {}".format(tapes_to_remove))

    if len(tapes) == 0:
        logger.error("No free Tapes in Library, but you can remove these full once: {}".format(tapes_to_remove))
        sys.exit(0)

    next_tape = tapes.pop(0)

    logger.info("Using tape {} for writing".format(next_tape))

    ## Load tape, mount and maybe format tapedevice
    tapelibrary.load(next_tape)
    tapelibrary.ltfs()

    ## Write used tape into database
    database.write_tape_into_database(next_tape)

    st = os.statvfs(cfg['local-tape-mount-dir'])
    logger.info("Tape: Used: {} ({} GB), Free: {} ({} GB), Total: {} ({} GB)".format(
                                                            (st.f_blocks - st.f_bfree) * st.f_frsize,
                                                            int((st.f_blocks - st.f_bfree) * st.f_frsize / 1024 / 1024 / 1024),
                                                            (st.f_bavail * st.f_frsize),
                                                            int((st.f_bavail * st.f_frsize) / 1024 / 1024 / 1024),
                                                            (st.f_blocks * st.f_frsize),
                                                            int((st.f_blocks * st.f_frsize) / 1024 / 1024 / 1024)
    ))

    files = database.get_files_to_be_written()
    for file in files:
        id = file[0]
        filename = file[1]
        md5 = file[2]
        orig_filename = file[3]

        ##Get free tapesize, filesize and compare with a space blocker of 1GB and test 5% of the written media
        st = os.statvfs(cfg['local-tape-mount-dir'])
        free = (st.f_bavail * st.f_frsize)
        filesize = os.path.getsize("{}/{}".format(cfg['local-enc-dir'], filename))

        logger.debug("Tape: Free: {}, Used: {}, Fileid: {}, Filesize: {}".format(
            free,
            int((st.f_blocks - st.f_bfree) * st.f_frsize),
            id,
            filesize
        ))

        if filesize > ( free - 10737418240 ):
            logger.debug("Writing fileid {} to tape".format(id))
            logger.warning("Tape is full: I am testing now a few media, writing summary into database and unloading tape")

            if not test_backup_pieces(database.get_files_by_tapelabel(next_tape), 5):
                logger.error("md5sum on tape not equal to database. Stopping everything. Need manual check of the tape!")
                ## TODO: Mache irgendwas vernünftiges!
                sys.exit(1)

            database.mark_tape_as_full(next_tape, int(time.time()))

            ## WRITE Database encrypted on tape
            dt = int(time.time())
            command = ['openssl', 'enc', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in', cfg['database'], '-out', '{}/tapebackup_{}.db.enc'.format(cfg['local-tape-mount-dir'], dt), '-k', cfg['enc-key']]
            openssl = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if len(openssl.stderr.readlines()) > 0:
                logger.error("Writing Database to Tape failed")
                sys.exit(1)

            ## WRITE Textfile containing (encryped_name|original_fullpath) of all files encrypted to tape
            dump = database.dump_filenames_to_for_tapes(next_tape)
            with open('tapebackup_{}.txt'.format(dt), 'w') as f:
                for line in dump:
                    f.write('"{}";"{}";"{}"\n'.format(line[0], line[1], line[2]))
            command = ['openssl', 'enc', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in', 'tapebackup_{}.txt'.format(dt), '-out', '{}/tapebackup_{}.txt.enc'.format(cfg['local-tape-mount-dir'], dt), '-k', cfg['enc-key']]
            openssl = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if len(openssl.stderr.readlines()) > 0:
                logger.error("Writing Textfile to Tape failed")
                sys.exit(1)
            os.remove('tapebackup_{}.txt'.format(dt))

            ## DELETE all Files, that has been transfered to tape
            for i in database.get_files_by_tapelabel(next_tape):
                if os.path.exists("{}/{}".format(cfg['local-enc-dir'], i[1])):
                    logger.info("Deleting encrypted file: {} ({})".format(i[3], i[1]))
                    os.remove("{}/{}".format(cfg['local-enc-dir'], i[1]))

            ## Unload tape
            tapelibrary.unload()
            recursive = True
            break

        logger.info("Writing file to tape: {}".format(orig_filename))
        shutil.copy2("{}/{}".format(cfg['local-enc-dir'], filename), "{}/".format(cfg['local-tape-mount-dir']))
        database.update_file_after_write(int(time.time()), next_tape, id)

        if interrupted:
            break

    #if recursive:
    #    write_files()


def restore_file():
    ## TODO: Restore file by given name, path or encrypted name
    pass

def verify_file():
    ## TODO: Verify random or by given File
    pass

def verify_tape():
    ## TODO: - Verify random or by given Tape
    ##       - Verify filesystem and my be a few files
    ##       - Verify Tapebackup Database file
    pass

parser = argparse.ArgumentParser(description="Tape backup from remote or local server to tape library")
parser.add_argument("-v", "--version", action="store_true", help="Show version and exit")

group01 = parser.add_argument_group()
group01.add_argument("--debug", action="store_true", help="Set log level to debug")
group01.add_argument("--info", action="store_true", help="Set log level to info")
group01.add_argument("--quiet", action="store_true", help="Set log level to error")

group02 = parser.add_argument_group()
group02.add_argument("--local", action="store_true", help="Use 'local-data-dir' as data source, not syncing from remote server, only adding to database and not deleting source files")
group02.add_argument("-c", "--config", type=str, help="Specify configuration yaml file [Default: config.yml]")
group02.add_argument("-D", "--database", type=str, help="Specify database [Default: Read from config file]")
group02.add_argument("-s", "--server", type=str, help="Specify remote server [Default: Read from config file]")
group02.add_argument("-d", "--data-dir", type=str, help="Specify 'local data directory' [Default: Read from config file]")
group02.add_argument("-l", "--tapelib", type=str, help="Specify tape library device [Default: Read from config file]")
group02.add_argument("-t", "--tapedrive", type=str, help="Specify tape drive device [Default: Read from config file]")
group02.add_argument("-m", "--tape-mount", type=str, help="Specify 'tape mount directory' [Default: Read from config file]")

subparsers = parser.add_subparsers(title='Commands', dest='command')
subparser_get = subparsers.add_parser('get', help='Get Files from remote Server')
subparser_encrypt = subparsers.add_parser('encrypt', help='Enrypt files and build directory for one tape media size')
subparser_write = subparsers.add_parser('write', help='Write directory into')
subparser_verify = subparsers.add_parser('verify', help='Verify Files (random or given filename) on Tape')
subparser_restore = subparsers.add_parser('restore', help='Restore File from Tape')
subparser_restore.add_argument("-f", "--file", type=str, required=True, help="Specify filename or path/file")


subparser_db = subparsers.add_parser('db', help='Database operations')
subsubparser_db = subparser_db.add_subparsers(title='Subcommands', dest='command_sub')
subsubparser_db.add_parser('init', help='Initialize SQLite DB')
subsubparser_db.add_parser('repair', help='Repair SQLite DB after stopped operation')
subsubparser_db.add_parser('backup', help='Backup SQLite DB to given GIT repo')
subsubparser_db.add_parser('status', help='Show SQLite Information')


subparser_tape = subparsers.add_parser('tape', help='Tapelibrary operations')
subsubparser_tape = subparser_tape.add_subparsers(title='Subcommands', dest='command_sub')
subsubparser_tape.add_parser('info', help='Get Informations about Tapes and Devices')
subsubparser_tape.add_parser('status', help='Get Informations about Tapes (offline/online and to be removed)')


subparser_config = subparsers.add_parser('config', help='Configuration operations')
subsubparser_config = subparser_config.add_subparsers(title='Subcommands', dest='command_sub')
subsubparser_config.add_parser('create_key', help='Create 128 Byte encryption key')


subparser_debug = subparsers.add_parser('debug', help='Print debug information')

if __name__ == "__main__":
    ## TODO: Make possible to run script from every path

    files_prefix = os.path.abspath(os.path.dirname(sys.argv[0]))
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.info:
        logging.getLogger().setLevel(logging.INFO)
    elif args.quiet:
        logging.getLogger().setLevel(logging.ERROR)

    if args.version:
        show_version()
        sys.exit(0)


    if args.config is not None:
        cfgfile = "{}/{}".format(files_prefix, args.config)
    elif debug:
        cfgfile = "{}/config-debug.yml".format(files_prefix)
    else:
        cfgfile = "{}/config.yml".format(files_prefix)

    with open(cfgfile, 'r') as ymlfile:
        cfg = yaml.full_load(ymlfile)


    if not os.path.isfile(cfg['database']) and args.command != "initDB" and args.command != "createKey" and args.command != "debug":
        logger.error("Database does not exist: {}. Please execute './main.py db init' first".format(cfg['database']))
        sys.exit(0)
    if ( cfg['enc-key'] == "" or len(cfg['enc-key']) < 128 ) and args.command != "initDB" and args.command != "createKey" and args.command != "debug":
        logger.error("Encryption key is empty, please use at least 128 Byte Key, use './main.py config create_key' to create a random key")
        sys.exit(0)

    if not os.path.isdir(cfg['local-data-dir']) and args.command != "initDB" and args.command != "createKey" and args.command != "debug":
        logger.error("'local-data-dir' not specified or does not exist")
        sys.exit(0)
    if not os.path.isdir(cfg['local-enc-dir']) and args.command != "initDB" and args.command != "createKey" and args.command != "debug":
        logger.error("'local-enc-dir' not specified or does not exist")
        sys.exit(0)

    if args.command == "initDB" and os.path.isfile(cfg['database']):
        logger.warning("Database file already exists. Just updating!")

    database = Database(cfg)
    tapelibrary = Tapelibrary(cfg, database)
    tools = Tools(cfg, database)

    if args.command == 'debug':
        print_debug_info()

    config_override_from_cmd()

    if args.command == "get":
        get_files()
    elif args.command == "encrypt":
        encrypt_files()
    elif args.command == "write":
        write_files()
    elif args.command == "verify":
        verify_file()
        ## verify_tape()
    elif args.command == "restore":
        restore_file()
    elif args.command == "tape":
        if args.command_sub == "info":
            tapeinfo()
        elif args.command_sub == "status":
            tapestatus()
        elif args.command_sub is None:
            parser.print_help()
    elif args.command == "db":
        if args.command_sub == "init":
            init_db()
        elif args.command_sub == "repair":
            repair_db()
        elif args.command_sub == "status":
            status_db()
        elif args.command_sub == "backup":
            if cfg['database-backup-git-path'] == "":
                logger.error("'database-backup-git-path' key is empty, please specify git path")
                sys.exit(0)
            backup_db()
        elif args.command_sub is None:
            parser.print_help()
    elif args.command == "db":
        if args.command_sub == "create_key":
            create_key()
        elif args.command_sub is None:
            parser.print_help()



