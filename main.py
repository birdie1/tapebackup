#!/usr/bin/env python3

import yaml
import sys
import logging
import argparse
import os
import signal
import psutil
from lib import database
from lib import Tapelibrary, Tools


pname = "Tapebackup"
pversion = '0.2'
db_model_version = 1
logger_format = '[%(levelname)-7s] (%(asctime)s) %(filename)s::%(lineno)d %(message)s'
log_dir = 'logs'
debug = False

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(level=logging.DEBUG,
                    format=logger_format,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=f"{log_dir}/main.log")
logger = logging.getLogger()

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter(logger_format)
handler.setFormatter(formatter)
logger.addHandler(handler)


def change_logger_filehandler(filename):
    for hdlr in logger.handlers:
        if isinstance(hdlr, logging.FileHandler):
            logger.removeHandler(hdlr)

    filehandler = logging.FileHandler(f"{log_dir}/{filename}")
    filehandler.setFormatter(
        logging.Formatter(logger_format)
    )
    logger.addHandler(filehandler)


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


def check_requirements():
    if cfg['enc-key'] == "" or len(cfg['enc-key']) < 128:
        logger.error(
            "Encryption key is empty, please use at least 128 Byte Key, use './main.py config create_key' to create a random key")
        sys.exit(0)

    if not os.path.isdir(cfg['local-data-dir']):
        logger.error("'local-data-dir' not specified or does not exist")
        sys.exit(0)
    if not os.path.isdir(cfg['local-enc-dir']):
        logger.error("'local-enc-dir' not specified or does not exist")
        sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tape backup from remote or local server to tape library")
    parser.add_argument("-v", "--version", action="store_true", help="Show version and exit")

    group01 = parser.add_argument_group()
    group01.add_argument("--debug", action="store_true", help="Set log level to debug")
    group01.add_argument("--info", action="store_true", help="Set log level to info")
    group01.add_argument("--quiet", action="store_true", help="Set log level to error")

    group02 = parser.add_argument_group()
    group02.add_argument("--local", action="store_true",
                         help="Use 'local-data-dir' as data source, not syncing from remote server, only adding to database and not deleting source files")
    group02.add_argument("-c", "--config", type=str, help="Specify configuration yaml file [Default: config.yml]")
    group02.add_argument("-D", "--database", type=str, help="Specify database [Default: Read from config file]")
    group02.add_argument("-s", "--server", type=str, help="Specify remote server [Default: Read from config file]")
    group02.add_argument("-d", "--data-dir", type=str,
                         help="Specify 'local data directory' [Default: Read from config file]")
    group02.add_argument("-l", "--tapelib", type=str,
                         help="Specify tape library device [Default: Read from config file]")
    group02.add_argument("-t", "--tapedrive", type=str,
                         help="Specify tape drive device [Default: Read from config file]")
    group02.add_argument("-m", "--tape-mount", type=str,
                         help="Specify 'tape mount directory' [Default: Read from config file]")

    subparsers = parser.add_subparsers(title='Commands', dest='command')
    subparser_get = subparsers.add_parser('get', help='Get Files from remote Server')
    subparser_encrypt = subparsers.add_parser('encrypt',
                                              help='Enrypt files and build directory for one tape media size')
    subparser_write = subparsers.add_parser('write', help='Write directory into')

    subparser_verify = subparsers.add_parser('verify', help='Verify Files (random or given filename) on Tape')
    subparser_verify_group = subparser_verify.add_mutually_exclusive_group(required=True)
    subparser_verify_group.add_argument("-f", "--file", type=str, nargs='?', const='',
                                        help="[Default: random file] or specify filename or path/file (Wildcards possible)")
    subparser_verify_group.add_argument("-t", "--tape", type=str, nargs='?', const='',
                                        help="[Default: random tape] or specify filename or path/file (Wildcards possible)")
    subparser_verify.add_argument("-c", "--count", type=int, default=1,
                                  help="[Only if no file/tape specified] Specify max number of files/tapes that will be verified (0 = unlimited) [Default: 1]")

    subparser_restore = subparsers.add_parser('restore', help='Restore File from Tape')
    subparser_restore_sub = subparser_restore.add_subparsers(title='Subcommands', dest='command_sub')
    subparser_restore_start = subparser_restore_sub.add_parser('start', help='Start restore operation (-f must be given)')
    subparser_restore_start.add_argument('-t', '--tape', type=str, help='Only restore from this tape')
    subparser_restore_start.add_argument('-l', '--filelist', type=str, help='Read paths from file list to restore')
    subparser_restore_start.add_argument('files', nargs='*', help='Select files by absolute path or with wildcard')
    subparser_restore_continue = subparser_restore_sub.add_parser('continue', help='Restore job will be continued')
    subparser_restore_continue.add_argument('jobid', nargs='?', help='Display status of specific restore job')
    subparser_restore_abort = subparser_restore_sub.add_parser('abort', help='Abort restore (delete from transactions db table)')
    subparser_restore_abort.add_argument('jobid', nargs='?', help='Display status of specific restore job')
    subparser_restore_sub.add_parser('list', help='List restore jobs')
    subparser_restore_status = subparser_restore_sub.add_parser('status', help='Print restore job status')
    subparser_restore_status.add_argument("-v", "--verbose", action="store_true", dest='verbose_list', help="Additionally print files in this restore job")
    subparser_restore_status.add_argument('jobid', nargs='?', help='Display status of specific restore job')

    subparser_files = subparsers.add_parser('files', help='File operations')
    subparser_files_sub = subparser_files.add_subparsers(title='Subcommands', dest='command_sub')
    subparser_files_list = subparser_files_sub.add_parser('list', help='Show files')
    subparser_files_list.add_argument("-v", "--verbose", action="store_true", dest='verbose_list', help="Print a verbose list with all database fields")
    subparser_files_list.add_argument("-t", "--tape", type=str, help="Only show files on a specific tape")
    subparser_files_list.add_argument('files', nargs='*', help='Filter files by absolute path or with wildcard')
    subparser_files_sub.add_parser('duplicate', help='Show duplicate files')
    subparser_files_sub.add_parser('summary', help='Show summary about files')

    subparser_log = subparsers.add_parser('log', help='Log operations')
    subparser_log_sub = subparser_log.add_subparsers(title='Subcommands', dest='command_sub')
    subparser_log_sub.add_parser('rotate', help='Rotate log files (Only files > 10MB)')
    subparser_log_sub.add_parser('remove_debug', help='Remove debug output from non rotated log files (This will shorten the log)')

    subparser_db = subparsers.add_parser('db', help='Database operations')
    subsubparser_db = subparser_db.add_subparsers(title='Subcommands', dest='command_sub')
    subsubparser_db.add_parser('repair', help='Repair SQLite DB after stopped operation')
    subsubparser_db.add_parser('backup', help='Backup SQLite DB to given GIT repo')
    subsubparser_db.add_parser('status', help='Show SQLite Information')
    subsubparser_db.add_parser('migrate', help='Migrate database from schema pre version 0.3')

    subparser_tape = subparsers.add_parser('tape', help='Tapelibrary operations')
    subsubparser_tape = subparser_tape.add_subparsers(title='Subcommands', dest='command_sub')
    subsubparser_tape.add_parser('info', help='Get Informations about Tapes and Devices')
    subsubparser_tape.add_parser('status', help='Get Informations about Tapes (offline/online and to be removed)')

    subparser_config = subparsers.add_parser('config', help='Configuration operations')
    subsubparser_config = subparser_config.add_subparsers(title='Subcommands', dest='command_sub')
    subsubparser_config.add_parser('create_key', help='Create 128 Byte encryption key')

    subparser_debug = subparsers.add_parser('debug', help='Print debug information')
    subparser_develop = subparsers.add_parser('develop', help='Generic function for developing new stuff')

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

    if args.command != "db" and args.command != "config" and args.command != "debug":
        check_requirements()
    else:
        if args.command_sub != "create_key":
            check_requirements()

    # Init database
    db_engine = database.init(cfg['database'], db_model_version)
    if not db_engine:
        if args.command == "db":
            if args.command_sub != "migrate" and args.command_sub != "upgrade":
                sys.exit(1)
        else:
            sys.exit(1)

    tapelibrary = Tapelibrary(cfg)
    tools = Tools(cfg)

    if args.command == 'debug':
        print_debug_info()

    config_override_from_cmd()

    if args.command == "get":
        logger.info("Starting get operation, logging into logs/get.log")
        change_logger_filehandler('get.log')
        logger.info("########## NEW SESSION ##########")

        from functions.files import Files
        current_class = Files(cfg, db_engine, tapelibrary, tools, args.local)
        current_class.get()

    elif args.command == "encrypt":
        logger.info("Starting encrypt operation, logging into logs/encrypt.log")
        change_logger_filehandler('encrypt.log')
        logger.info("########## NEW SESSION ##########")

        from functions.encryption import Encryption
        current_class = Encryption(cfg, db_engine, tapelibrary, tools, args.local)
        current_class.encrypt()

    elif args.command == "write":
        logger.info("Starting write operation, logging into logs/write.log")
        change_logger_filehandler('write.log')
        logger.info("########## NEW SESSION ##########")

        from functions.tape import Tape
        current_class = Tape(cfg, db_engine, tapelibrary, tools)
        current_class.write()

    elif args.command == "verify":
        logger.info("Starting verify operation, logging into logs/verify.log")
        change_logger_filehandler('verify.log')
        logger.info("########## NEW SESSION ##########")

        from functions.verify import Verify
        current_class = Verify(cfg, db_engine, tapelibrary, tools)
        if args.tape is None:
            current_class.file(args.file, args.count)
        elif args.file is None:
            current_class.tape(args.tape, args.count)

    elif args.command == "restore":
        logger.info("Starting restore operation, logging into logs/restore.log")
        change_logger_filehandler('restore.log')
        logger.info("########## NEW SESSION ##########")

        from functions.restore import Restore
        current_class = Restore(cfg, db_engine, tapelibrary, tools)

        if args.command_sub == "start":
            if not args.files and args.filelist is None:
                logger.error("You must specifiy either files or a list of files to restore")
                subparser_restore_start.print_help()
                sys.exit(1)
            else:
                current_class.start(args.files, tape=args.tape, filelist=args.filelist)
        elif args.command_sub == "continue":
            current_class.cont()
        elif args.command_sub == "abort":
            current_class.abort(args.jobid)
        elif args.command_sub == "status":
            current_class.status(args.jobid, args.verbose_list)
        elif args.command_sub == "list":
            current_class.list()
        elif args.command_sub is None:
            subparser_restore.print_help()

    elif args.command == "files":
        logger.info("Starting files operation, logging into logs/files.log")
        change_logger_filehandler('files.log')
        logger.info("########## NEW SESSION ##########")

        from functions.files import Files
        current_class = Files(cfg, db_engine, tapelibrary, tools)

        if args.command_sub == "list":
            current_class.list(args.files, args.verbose_list, args.tape)
        elif args.command_sub == "duplicate":
            current_class.duplicate()
        elif args.command_sub == "summary":
            current_class.summary()
        elif args.command_sub is None:
            subparser_files.print_help()

    elif args.command == "log":
        logger.info("Starting log operation, logging into logs/logging.log")
        change_logger_filehandler('logging.log')
        logger.info("########## NEW SESSION ##########")

        from functions.log import Log
        current_class = Log(cfg)

        if args.command_sub == "rotate":
            current_class.rotate(log_dir, logger_format)
        elif args.command_sub == "remove_debug":
            current_class.remove_debug(log_dir)

    elif args.command == "tape":
        logger.info("Starting tape operation, logging into logs/tape.log")
        change_logger_filehandler('tape.log')
        logger.info("########## NEW SESSION ##########")

        from functions.tape import Tape
        current_class = Tape(cfg, db_engine, tapelibrary, tools)

        if args.command_sub == "info":
            current_class.info()
        elif args.command_sub == "status":
            current_class.status()
        elif args.command_sub is None:
            subparser_tape.print_help()

    elif args.command == "db":
        logger.info("Starting db operation, logging into logs/db.log")
        change_logger_filehandler('db.log')
        logger.info("########## NEW SESSION ##########")

        from functions.db import Db
        current_class = Db(cfg, db_engine, tapelibrary, tools)
        if args.command_sub == "repair":
            current_class.repair()
        elif args.command_sub == "status":
            current_class.status()
        elif args.command_sub == "backup":
            if cfg['database-backup-git-path'] == "":
                logger.error("'database-backup-git-path' key is empty, please specify git path")
                sys.exit(0)
            current_class.backup()
        elif args.command_sub == "migrate":
            current_class.migrate(db_model_version)
        elif args.command_sub is None:
            subparser_db.print_help()

    elif args.command == "config":
        if args.command_sub == "create_key":
            create_key()
        elif args.command_sub is None:
            subparser_config.print_help()

    elif args.command == "develop":
        logger.info("Starting develop operation, logging from now on into logs/develop.log")
        change_logger_filehandler('develop.log')
        logger.info("########## NEW SESSION ##########")
        logger.info("Test 123")

        session = database.create_session(db_engine)
        file = database.get_restore_job_stats_total(session, 1)

        ## For debugging / programming pruspose only
        #from functions.develop import Develop

        #current_class = Develop(cfg, database, tapelibrary, tools)
        #current_class.current_test()
    else:
        parser.print_help()
