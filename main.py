#!/usr/bin/env python3


import sqlite3
import yaml
import sys
import logging
import argparse
import os
import time
import signal
import subprocess
import hashlib
import secrets
import string
from functools import partial
from sqlite3 import Error

logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)-7s] (%(asctime)s) %(filename)s::%(lineno)d %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='main.log')
logger = logging.getLogger()

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.WARNING)
formatter = logging.Formatter('[%(levelname)-7s] (%(asctime)s) %(filename)s::%(lineno)d %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

files_prefix = os.path.abspath(os.path.dirname(sys.argv[0]))

def signal_handler(signal, frame):
    global interrupted
    global child_process_pid
    if interrupted:
        print('Pressed CTRL + C twice, giving up. Please check the database for broken entry!')
        os.kill(child_process_pid, signal)
    else:
        interrupted = True
        print('I will stop after current Operation!')

signal.signal(signal.SIGINT, signal_handler)
interrupted = False
child_process_pid = 0



with open("{}/config.yml".format(files_prefix), 'r') as ymlfile:
#with open("{}/config-kiste.yml".format(files_prefix), 'r') as ymlfile:
    cfg = yaml.full_load(ymlfile)


def md5sum(filename):
    with open(filename, mode='rb') as f:
        d = hashlib.md5()
        for buf in iter(partial(f.read, 4096), b''):
            d.update(buf)
    return d.hexdigest()


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def init_db():

    if os.path.isfile(cfg['database']):
        logger.warning("Database file already exists. Just updating!")

    conn = create_connection(cfg['database'])
    c = conn.cursor()

    sql_files = '''CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            filename TEXT NOT NULL,
            full_path TEXT NOT NULL UNIQUE,
            filename_encrypted TEXT UNIQUE,
            mtime TEXT,
            md5sum_file TEXT,
            md5sum_encrypted TEXT,
            tape TEXT,
            downloaded_date TEXT,
            packed_date TEXT,
            written_date TEXT,
            downloaded INT DEFAULT 0,
            packed INT DEFAULT 0,
            written INT DEFAULT 0,
            verified INT DEFAULT 0     
            );'''

    sql_tapedevice = '''CREATE TABLE IF NOT EXISTS tapedevices (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            full_date TEXT,
            files_count INT DEFAULT 0,
            full INT DEFAULT 0
            );'''

    alternative_file_names = '''CREATE TABLE IF NOT EXISTS alternative_file_names (
            id INTEGER PRIMARY KEY,
            filename TEXT NOT NULL UNIQUE,
            files_id INT NOT NULL,
            date TEXT 
            );'''

    try:
        c.execute(sql_files)
        c.execute(sql_tapedevice)
        c.execute(alternative_file_names)
    except Error as e:
        print(e)


def total_rows(cursor, table_name, print_out=False):
    """ Returns the total number of rows in the database """
    cursor.execute('SELECT COUNT(*) FROM {}'.format(table_name))
    count = cursor.fetchall()
    if print_out:
        print('\nTotal rows: {}'.format(count[0][0]))
    return count[0][0]


def table_col_info(cursor, table_name, print_out=False):
    """ Returns a list of tuples with column informations:
    (id, name, type, notnull, default_value, primary_key)
    """
    cursor.execute('PRAGMA TABLE_INFO({})'.format(table_name))
    info = cursor.fetchall()

    if print_out:
        print("\nColumn Info:\nID, Name, Type, NotNull, DefaultVal, PrimaryKey")
        for col in info:
            print(col)
    return info


def values_in_col(cursor, table_name, print_out=True):
    """ Returns a dictionary with columns as keys
    and the number of not-null entries as associated values.
    """
    cursor.execute('PRAGMA TABLE_INFO({})'.format(table_name))
    info = cursor.fetchall()
    col_dict = dict()
    for col in info:
        col_dict[col[1]] = 0
    for col in col_dict:
        cursor.execute('SELECT ({0}) FROM {1} '
                  'WHERE {0} IS NOT NULL'.format(col, table_name))
        # In my case this approach resulted in a
        # better performance than using COUNT
        number_rows = len(cursor.fetchall())
        col_dict[col] = number_rows
    if print_out:
        print("\nNumber of entries per column:")
        for i in col_dict.items():
            print('{}: {}'.format(i[0], i[1]))
    return col_dict


def insert_file(conn, file):
    """
    Create a new project into the projects table
    :param conn:
    :param file:
    :return: file id
    """
    sql = ''' INSERT INTO files(filename,full_path)
              VALUES(?,?) '''
    cur = conn.cursor()
    cur.execute(sql, file)
    conn.commit()
    return cur.lastrowid


def check_if_file_exists_by_path(conn, relpath, filename):
    cur = conn.cursor()
    cur.execute("SELECT * FROM files WHERE full_path=?", (relpath,))
    rows_files = cur.fetchall()

    cur2 = conn.cursor()
    cur2.execute("SELECT * FROM alternative_file_names WHERE filename=?", (filename,))
    rows_alt_files = cur2.fetchall()

    if len(rows_files) == 0 and len(rows_alt_files) == 0:
        return False
    else:
        return True


def get_files_to_be_packed(conn):
    cur = conn.cursor()
    sql = ''' SELECT id, filename, full_path FROM files 
            WHERE downloaded=1
            AND packed = 0
            '''
    cur.execute(sql)
    return cur.fetchall()


def update_file_after_download(conn, task):
    sql = ''' UPDATE files
              SET mtime = ?,
                  downloaded_date = ?,
                  md5sum_file = ?,
                  downloaded = ?
              WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()


def get_files_by_md5(conn, md5):
    cur = conn.cursor()
    sql = ''' SELECT id FROM files
                WHERE md5sum_file=?
                '''
    cur.execute(sql, (md5,))
    return cur.fetchall()


def insert_alternative_file_names(conn, file):
    sql = ''' INSERT INTO alternative_file_names (filename,files_id,date)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, file)
    conn.commit()
    return cur.lastrowid


def update_filename_enc(conn, filename_enc):
    sql = ''' UPDATE files
              SET filename_encrypted = ?
              WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, filename_enc)
    conn.commit()


def update_file_after_pack(conn, task):
    sql = ''' UPDATE files
                  SET packed_date = ?,
                      md5sum_encrypted = ?,
                      packed = ?
                  WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()


def get_broken_db_download_entry(conn):
    cur = conn.cursor()
    sql = ''' SELECT id, full_path FROM files
                WHERE downloaded=0
                '''
    cur.execute(sql)
    return cur.fetchall()


def delete_broken_db_download_entry(conn, id):
    sql = ''' DELETE from files
                  WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()


def get_broken_db_pack_entry(conn):
    cur = conn.cursor()
    sql = ''' SELECT id, filename_encrypted FROM files 
                WHERE filename_encrypted IS NOT NULL
                and packed=0
                '''
    cur.execute(sql)
    return cur.fetchall()


def update_broken_db_pack_entry(conn, id):
    sql = ''' UPDATE files
                 SET filename_encrypted = NULL
                 WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()


def get_full_tapes(conn, label):
    cur = conn.cursor()
    sql = ''' SELECT id, label, full FROM tapedevices 
            WHERE label=?
            AND full=1
            '''
    cur.execute(sql, (label,))
    return cur.fetchall()


def get_used_tapes(conn, label):
    cur = conn.cursor()
    sql = ''' SELECT id, label, full FROM tapedevices 
            WHERE label=?
            AND full=0
            '''
    cur.execute(sql, (label,))
    return cur.fetchall()


def strip_base_path(fullpath):
    return os.path.relpath(fullpath, cfg['remote-base-dir'])


def strip_path(path):
    return os.path.basename(path)


def strip_filename(path):
    return os.path.dirname(path)


def get_tapes_tags_from_library(conn):
    logger.info("Retrieving current tape tags in library")
    commands = ['mtx', '-f', cfg['devices']['tapelib'] , 'status']
    mtx = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    tag_in_tapelib = []
    tags_to_remove_from_library = []

    for i in mtx.stdout.readlines():
        line = i.decode('utf-8').rstrip()
        if line.find('VolumeTag') != -1:
            tag = line[line.find('=') + 1:].rstrip().lstrip()
            if tag in cfg['lto-ignore-tapes']:
                logger.debug('Ignore Tag {} because exists in ignore list in config'.format(tag))
            elif len(get_full_tapes(conn, tag)) > 0:
                logger.debug('Ignore Tag {} because exists in database and is full'.format(tag))
                tags_to_remove_from_library.append(tag)
            else:
                tag_in_tapelib.append(tag)

    logger.info("Got following tags for usage: {}".format(tag_in_tapelib))
    return tag_in_tapelib, tags_to_remove_from_library


## main functions from here:
def create_key():
    alphabet = string.ascii_letters + string.digits
    print(''.join(secrets.choice(alphabet) for i in range(128)))


def repair_db():
    conn = create_connection(cfg['database'])

    broken_d = get_broken_db_download_entry(conn)
    for file in broken_d:
        if os.path.isfile("{}/{}".format(cfg['local-download-dir'], file[1])):
            os.remove("{}/{}".format(cfg['local-download-dir'], file[1]))

        logger.info("Fixing Database ID: {}".format(file[0]))
        delete_broken_db_download_entry(conn, file[0])

    logger.info("Fixed {} messed up download entries".format(len(broken_d)))
    print("Fixed {} messed up download entries".format(len(broken_d)))


    broken_p = get_broken_db_pack_entry(conn)
    for file in broken_p:
        if os.path.isfile("{}/{}".format(cfg['local-enc-dir'], file[1])):
            os.remove("{}/{}".format(cfg['local-enc-dir'], file[1]))

        logger.info("Fixing Database ID: {}".format(file[0]))
        update_broken_db_pack_entry(conn, file[0])

    logger.info("Fixed {} messed up pack entries".format(len(broken_p)))
    print("Fixed {} messed up pack entries".format(len(broken_p)))


def status_db():
    conn = create_connection(cfg['database'])
    c = conn.cursor()
    tables = ['files', 'tapedevices', 'alternative_file_names']

    for i in tables:
        print("")
        print("######### SHOW TABLE {} ##########".format(i))
        total_rows(c, i, print_out=True)
        table_col_info(c, i, print_out=True)
        values_in_col(c, i, print_out=True)


def backup_db():
    ## Somethng like this. Then git stuff
    #conn = create_connection(cfg['database'])
    #with open('dump.sql', 'w') as f:
        #for line in con.iterdump():
            #f.write('%s\n' % line)
    pass


def get_files():
    global interrupted
    global child_process_pid

    logger.info("Retrieving file list from server {} directory '{}'".format(cfg['remote-server'], cfg['remote-download-dir']))
    commands = ['ssh', cfg['remote-server'], 'find "{}" -type f'.format(cfg['remote-download-dir'])]
    ssh = subprocess.Popen(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result = ssh.stdout.readlines()
    logger.info("Got file list from server {} directory '{}'".format(cfg['remote-server'], cfg['remote-download-dir']))

    conn = create_connection(cfg['database'])
    file_count_total = len(result)
    print("Found {} entries. Start to process.".format(file_count_total))
    logger.info("Found {} entries. Start to process.".format(file_count_total))

    for fpath in result:
        fullpath = fpath.decode("UTF-8").rstrip()
        logger.info("Processing {}".format(fullpath))

        relpath = strip_base_path(fullpath)
        filename = strip_path(fullpath)
        dir = strip_filename(relpath)

        if not check_if_file_exists_by_path(conn, relpath, filename):
            id = insert_file(conn, (filename, relpath))
            logger.info("Inserting file into database. Fileid: {}".format(id))
            print("Processing {}".format(fullpath))

            os.makedirs("{}/{}".format(cfg['local-download-dir'], dir), exist_ok=True)

            command = ['rsync', '--protect-args', '-ae', 'ssh', '{}:{}'.format(cfg['remote-server'], fullpath), '{}/{}'.format(cfg['local-download-dir'], dir)]
            rsync = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setpgrp)
            child_process_pid = rsync.pid
            #print(rsync.args)
            #print(rsync.stderr.readlines())

            if len(rsync.stderr.readlines()) == 0:
                mtime = os.path.getmtime("{}/{}".format(cfg['local-download-dir'], relpath))
                md5 = md5sum("{}/{}".format(cfg['local-download-dir'], relpath))
                downloaded_date = int(time.time())

                duplicate = get_files_by_md5(conn, md5)
                if len(duplicate) > 0:
                    logger.info("File downloaded with another name. Storing filename in Database: {}".format(filename))
                    print("File downloaded with another name. Storing filename in Database: {}".format(filename))
                    duplicate_id = duplicate[0][0]
                    inserted_id = insert_alternative_file_names(conn, (filename, duplicate_id, downloaded_date,))
                    delete_broken_db_download_entry(conn, id)
                    os.remove("{}/{}".format(cfg['local-download-dir'], relpath))
                else:
                    update_file_after_download(conn, (mtime, downloaded_date, md5, 1, id))
                    logger.info("Download finished: {}".format(relpath))
            else:
                logger.warning("Download failed, file: {} error: {}".format(relpath, rsync.stderr.readlines()))
        else:
            logger.info("File already downloaded, skipping {}".format(relpath))

        if interrupted:
            break


def pack_files():
    global interrupted
    global child_process_pid

    logger.info("Starting pack files job")

    conn = create_connection(cfg['database'])
    files = get_files_to_be_packed(conn)
    alphabet = string.ascii_letters + string.digits

    for file in files:
        id = file[0]
        filepath = file[2]

        logger.info("Processing fileid: {}".format(id))
        print("Processing {}".format(file[1]))

        filename_enc_helper = ''.join(secrets.choice(alphabet) for i in range(32))
        filename_enc = "{}.enc".format(filename_enc_helper)

        update_filename_enc(conn, (filename_enc, id))

        command = ['openssl', 'enc', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in', '{}/{}'.format(cfg['local-download-dir'], filepath), '-out', '{}/{}'.format(cfg['local-enc-dir'], filename_enc), '-k', cfg['enc-key']]
        openssl = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setpgrp)
        child_process_pid = openssl.pid

        if len(openssl.stderr.readlines()) == 0:
            md5 = md5sum("{}/{}".format(cfg['local-enc-dir'], filename_enc))
            packed_date = int(time.time())
            update_file_after_pack(conn, (packed_date, md5, 1, id))

            os.remove("{}/{}".format(cfg['local-download-dir'], filepath))
        else:
            logger.warning("pack file failed, file: {} error: {}".format(id, openssl.stderr.readlines()))

        if interrupted:
            break

    ## encrypt
    # openssl enc -aes-256-cbc -pbkdf2 -iter 100000 -in '2016-09-23_dmax_Ice Lake Rebels; Bären auf dem See_AVC_1280x720_1600_AAC LC_128.mp4' -out test.enc -k supersicherespasswort
    ## decrypt
    # openssl enc -d -aes-256-cbc -pbkdf2 -iter 100000 -in test.enc -out test.mp4


def write_files():
    conn = create_connection(cfg['database'])
    tapes, tapes_to_remove = get_tapes_tags_from_library(conn)
    if len(tapes_to_remove) > 0:
        print("These tapes are full, please remove from library: {}".format(tapes_to_remove))
        logger.warning("These tapes are full, please remove from library: {}".format(tapes_to_remove))

    if len(tapes) == 0:
        logger.error("No free Tapes in Library, but you can remove these full once: {}".format(tapes_to_remove))
        sys.exit(0)

    ## do folder of 1,3tb encrypted filed
    ## see if any angefangene bänder, dann auch kleinere folder machen
    ##get_used_tapes(conn, tag)

    ##do more stuff here

def restore_file():
    pass


parser = argparse.ArgumentParser(description="Tape Backup from Remote Server to Tape Library by chunks")
parser.add_argument("--version", action="store_true", help="Show version and exit")

group01 = parser.add_argument_group()
group01.add_argument("--debug", action="store_true", help="Set log level to debug")
group01.add_argument("--info", action="store_true", help="Set log level to info")
group01.add_argument("--quiet", action="store_true", help="Set log level to error")


subparsers = parser.add_subparsers(title='Commands', dest='command')

subparser_get = subparsers.add_parser('get', help='Get Files from remote Server')
#subparser_dns.add_argument("-H", "--hostname", type=str, help="Specify hostname [Default: Build for all known firewalls]")
#subparser_dns.add_argument("-p", "--print", action="store_true", help="Print firewall commands [DEFAULT]")
#subparser_dns.add_argument("-s", "--sync", action="store_true", help="Sync firewall")
#subparser_dns.add_argument("-d", "--no-diff", action="store_true",
#                           help="Print all DNS objects, instead of a diff. Specify -H for specific firewall or IPAM DNS Objects will be displayed")
#subparser_dns.add_argument("-a", "--addresses-only", action="store_true",
#                           help="Processing addresses only (groups will be ignored)")
#subparser_dns.add_argument("-g", "--groups-only", action="store_true",
#                           help="Processing groups only (addresses will be ignored)")

subparser_pack = subparsers.add_parser('pack', help='Enrypt files and build directory for one tape media size')



subparser_write = subparsers.add_parser('write', help='Write directory into')


subparser_init = subparsers.add_parser('initDB', help='Initialize SQLite DB')
subparser_repair = subparsers.add_parser('repairDB', help='Repair SQLite DB after stopped operation')
subparser_backup = subparsers.add_parser('backupDB', help='Backup SQLite DB to given GIT repo')
subparser_dbstats = subparsers.add_parser('statusDB', help='Show SQLite Information')

#subparser_db = subparsers.add_parser('db', help='Database operations')
#subsubparser_db = subparser_db.add_subparsers(title='Commands', dest='command')
#subsubparser_db.add_parser('init', help='Initialize SQLite DB')

subparser_key = subparsers.add_parser('createKey', help='Create encryption key')

subparser_restore = subparsers.add_parser('restore', help='Restore File from Tape')
subparser_restore.add_argument("-f", "--file", type=str, help="Specify filename or path/file")

if __name__ == "__main__":

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.info:
        logging.getLogger().setLevel(logging.INFO)
    elif args.quiet:
        logging.getLogger().setLevel(logging.ERROR)

    if args.version:
        show_version()

    if not os.path.isfile(cfg['database']) and ( args.command != "initDB" or args.command != "createKey" ):
        logger.error("Database does not exist: {}".format(cfg['database']))
        sys.exit(0)
    if ( cfg['enc-key'] == "" or len(cfg['enc-key']) < 128 ) and ( args.command != "initDB" or args.command != "createKey" ):
        logger.error("Encryption key is empty, please use at least 128 Byte Key")
        sys.exit(0)

    if not os.path.isdir(cfg['local-download-dir']):
        logger.error("'local-download-dir' not specified or does not exist")
        sys.exit(0)
    if not os.path.isdir(cfg['local-enc-dir']):
        logger.error("'local-enc-dir' not specified or does not exist")
        sys.exit(0)



    if args.command == "get":
        get_files()
    elif args.command == "pack":
        pack_files()
    elif args.command == "write":
        write_files()
    elif args.command == "initDB":
        init_db()
    elif args.command == "createKey":
        create_key()
    elif args.command == "repairDB":
        repair_db()
    elif args.command == "statusDB":
        status_db()
    elif args.command == "backupDB":
        if cfg['database-backup-git-path'] == "":
            logger.error("'database-backup-git-path' key is empty, please specify git path")
            sys.exit(0)
        backup_db()
    elif args.command == "restore":
        restore_file()

