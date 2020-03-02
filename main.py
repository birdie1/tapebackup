#!/usr/bin/env python3

import yaml
import sys
import logging
import argparse
import os
import time
import signal
import psutil
from lib import Database, Tapelibrary, Tools


pname = "Tapebackup"
pversion = '0.1.2'
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


def signal_handler(signalo, frame):
    global interrupted
    global current_class
    if interrupted:
        print(' Pressed CTRL + C twice, giving up. Please check the database for broken entry!')

        parent = psutil.Process(os.getpid())
        children = parent.children(recursive=True)
        for process in children:
            process.send_signal(signal.SIGTERM)

        sys.exit(1)
    else:
        interrupted = True
        current_class.set_interrupted()
        print(' I will stop after current Operation!')

signal.signal(signal.SIGINT, signal_handler)
interrupted = False

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
    for j in cfg['lto-blacklist']:
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
    print(tools.create_encryption_key())


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


def fix_timestamp_db():
    fixed = 0
    files = database.get_all_files()
    for i in files:
        try:
            int(i[4])
        except TypeError:
            continue
        except ValueError:
            database.fix_float_timestamps(i[0], int(float(i[4])))
            fixed += 1

    logger.info("Fix Timestamps: fixed: {}, already ok: {}".format(fixed, len(files) - fixed))


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


parser = argparse.ArgumentParser(description="Tape backup from remote or local server to tape library")
parser.add_argument("-v", "--version", action="store_true", help="Show version and exit")

group01 = parser.add_argument_group()
group01.add_argument(
    "--debug",
    action="store_true",
    help="Set log level to debug"
)
group01.add_argument(
    "--info",
    action="store_true",
    help="Set log level to info"
)
group01.add_argument(
    "--quiet",
    action="store_true",
    help="Set log level to error"
)

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


subparser_verify = subparsers.add_parser(
    'verify',
    help='Verify Files (random or given filename) on Tape'
)
subparser_verify_group = subparser_verify.add_mutually_exclusive_group(required=True)
subparser_verify_group.add_argument(
    "-f",
    "--file",
    type=str,
    nargs='?',
    const='',
    help="[Default: random file] or specify filename or path/file (Wildcards possible)"
)
subparser_verify_group.add_argument(
    "-t",
    "--tape",
    type=str,
    nargs='?',
    const='',
    help="[Default: random tape] or specify filename or path/file (Wildcards possible)"
)
subparser_verify.add_argument(
    "-c",
    "--count",
    type=int,
    default=1,
    help="[Only if no file/tape specified] Specify max number of files/tapes that will be verified (0 = unlimited) [Default: 1]"
)


subparser_restore = subparsers.add_parser('restore', help='Restore File from Tape')
subparser_restore.add_argument("-f", "--file", type=str, required=True, help="Specify filename or path/file (Wildcards possible)")

subparser_files = subparsers.add_parser(
    'files',
    help='File operations'
)
#subparser_files.add_argument("-p", "--path", type=str, help="Specify path (Wildcards possible)")
subparser_files.add_argument("-s", "--short", action="store_true", help="Shorten output to interesting things")
subsubparser_files = subparser_files.add_subparsers(
    title='Subcommands',
    dest='command_sub'
)
subsubparser_files.add_parser(
    'list',
    help='Show files'
)
subsubparser_files.add_parser(
    'duplicate',
    help='Show duplicate files'
)


subparser_db = subparsers.add_parser('db', help='Database operations')
subsubparser_db = subparser_db.add_subparsers(title='Subcommands', dest='command_sub')
subsubparser_db.add_parser('init', help='Initialize SQLite DB')
subsubparser_db.add_parser('repair', help='Repair SQLite DB after stopped operation')
subsubparser_db.add_parser('fix_timestamp', help='Fix float timestamps from program version < 0.1.0')
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
    files_prefix = os.path.abspath(os.path.dirname(sys.argv[0]))
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        handler.setLevel(logging.DEBUG)
    elif args.info:
        logging.getLogger().setLevel(logging.INFO)
        handler.setLevel(logging.INFO)
    elif args.quiet:
        logging.getLogger().setLevel(logging.ERROR)
        handler.setLevel(logging.ERROR)

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
        from functions.files import Files
        current_class = Files(cfg, database, tapelibrary, tools)
        current_class.get()

    elif args.command == "encrypt":
        from functions.encryption import Encryption
        current_class = Encryption(cfg, database, tapelibrary, tools, args.local)
        current_class.encrypt()

    elif args.command == "write":
        from functions.tape import Tape
        current_class = Tape(cfg, database, tapelibrary, tools)
        current_class.write()

    elif args.command == "verify":
        from functions.verify import Verify
        current_class = Verify(cfg, database, tapelibrary, tools)
        if args.tape is None:
            current_class.file(args.file, args.count)
        elif args.file is None:
            current_class.tape(args.tape, args.count)

    elif args.command == "restore":
        from functions.encryption import Encryption
        current_class = Encryption(cfg, database, tapelibrary, tools, args.local)
        current_class.restore()

    elif args.command == "files":
        from functions.files import Files
        current_class = Files(cfg, database, tapelibrary, tools)

        if args.command_sub == "list":
            current_class.list(args.short)
        elif args.command_sub == "duplicate":
            current_class.duplicate()
        elif args.command_sub is None:
            parser.print_help()

    elif args.command == "tape":
        from functions.tape import Tape
        current_class = Tape(cfg, database, tapelibrary, tools)

        if args.command_sub == "info":
            current_class.info()
        elif args.command_sub == "status":
            current_class.status()
        elif args.command_sub is None:
            parser.print_help()

    elif args.command == "db":
        if args.command_sub == "init":
            init_db()
        elif args.command_sub == "repair":
            repair_db()
        elif args.command_sub == "fix_timestamp":
            fix_timestamp_db()
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



