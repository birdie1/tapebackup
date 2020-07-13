import sqlite3
import logging
import sys
import time
from sqlalchemy import create_engine
from sqlalchemy.ext.serializer import dumps
from sqlalchemy.orm import sessionmaker
from lib.models import File, Tape, RestoreJob, RestoreJobFileMap
from sqlite3 import Error

logger = logging.getLogger()


def connect(db):
    engine = create_engine(f"sqlite:///{db}")

    # Add table to database if not exist
    File.__table__.create(bind=engine, checkfirst=True)
    Tape.__table__.create(bind=engine, checkfirst=True)
    RestoreJob.__table__.create(bind=engine, checkfirst=True)
    RestoreJobFileMap.__table__.create(bind=engine, checkfirst=True)

    return engine


def create_session(engine):
    session = sessionmaker(bind=engine)
    return session()


def file_exists_by_path(session, relative_path):
    """
    Check if filename known in database
    :param session: orm session
    :param relative_path: relative file path
    :return: Boolean if found
    """
    return session.query(File).filter(File.path == relative_path).first()


def insert_file(session, filename, relative_path):
    """
    Create a new file entry
    :param session: orm session
    :param filename: file name
    :param relative_path: relative file path
    :return: inserted file id
    """
    file = File(filename=filename, path=relative_path)
    session.add(file)
    session.commit()
    return file


def get_file_by_md5(session, md5):
    return session.query(File).filter(File.md5sum_file == md5).first()


def update_file_after_download(session, file, filesize, mtime, downloaded_date, md5):
    """
    Update file object after download
    :param session: orm session
    :param file: file object
    :param filesize:
    :param mtime:
    :param downloaded_date:
    :param md5:
    :return:
    """
    file.filesize = filesize
    file.mtime = mtime
    file.downloaded_date = downloaded_date
    file.md5sum_file = md5
    file.downloaded = True
    session.commit()


def update_duplicate_file_after_download(session, file, duplicate_file, mtime, downloaded_date):
    """
    Add an alternative file if file already exists(by md5sum)
    :param session: orm session
    :param file: file object
    :param duplicate_file: file object of duplicate file with same md5sum
    :param mtime: mtime of file
    :param downloaded_date: downlodaded date of alternative file
    :return: file object
    """
    file.duplicate_id = duplicate_file.id
    file.mtime = mtime
    file.downloaded_date = downloaded_date
    session.commit()
    return file


def delete_broken_file(session, file):
    session.delete(file)
    session.commit()

def get_all_files(self):
    return self.session.query(File).all()


def get_tables(session):
    rows = session.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    return [table[0] for table in rows]


def total_rows(session, table_name, print_out=False):
    """ Returns the total number of rows in the database """
    count = session.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()
    if print_out:
        print('Total rows: {}'.format(count[0]))
    return count[0]


def table_col_info(session, table_name, print_out=False):
    """ Returns a list of tuples with column informations:
    (id, name, type, notnull, default_value, primary_key)
    """
    info = session.execute(f'PRAGMA TABLE_INFO({table_name})').fetchall()

    if print_out:
        print("Column Info:\n    ID, Name, Type, NotNull, DefaultVal, PrimaryKey")
        for col in info:
            print(f"    {col}")
    return info


def values_in_col(session, table_name, print_out=True):
    """ Returns a dictionary with columns as keys
    and the number of not-null entries as associated values.
    """
    info = session.execute('PRAGMA TABLE_INFO({})'.format(table_name)).fetchall()
    col_dict = dict()
    for col in info:
        col_dict[col[1]] = 0
    for col in col_dict:
        number_rows = len(session.execute(f'SELECT ({col}) FROM {table_name} \
                              WHERE {col} IS NOT NULL').fetchall())
        # In my case this approach resulted in a
        # better performance than using COUNT
        col_dict[col] = number_rows
    if print_out:
        print("Number of entries per column:")
        for i in col_dict.items():
            print(f'    {i[0]}: {i[1]}')
    return col_dict


def get_broken_db_download_entry(session):
    return session.query(File).filter(File.duplicate_id.is_(None), File.downloaded.is_(False)).all()


def get_broken_db_encrypt_entry(session):
    return session.query(File).filter(File.filename_encrypted.isnot(None), File.encrypted.is_(False)).all()


def update_broken_db_encrypt_entry(session, file):
    file.filename_encrypted = None
    session.commit()


def get_files_to_be_written(session):
    return session.query(File).filter(
        File.downloaded.is_(True),
        File.encrypted.is_(True),
        File.written.is_(False)
    ).all()

    # TODO: Remove this old style after everything is in sqlalchemy style
    #sql = '''SELECT id,
    #                 filename_encrypted,
    #                 filename,
    #                 encrypted_filesize
    #         FROM files
    #         WHERE downloaded = 1
    #         AND encrypted = 1
    #         AND written = 0'''


def get_not_deleted_files(session):
    return session.query(File).filter(File.deleted.is_(False)).all()


def set_file_deleted(session, file):
    file.deleted = True
    session.commit()
















class Database:




    # TODO: Need Rework
    #def export(self, filename):
    #    with open(filename, 'w') as f:
    #        #for line in self.conn.iterdump():
    #        #    f.write('{}\n'.format(line))

    def fetchall_from_database(self, sql, data=()):
        try_count = 0
        while True:
            try_count += 1
            try:
                self.cursor.execute(sql, data)
                break
            except sqlite3.OperationalError as e:
                if try_count == 10:
                    logger.warning("Database locked, giving up. ({}/10)".format(try_count))
                    sys.exit(1)
                else:
                    logger.warning("Database locked, waiting 10s for next try. ({}/10) [{}]".format(try_count, e))
                    time.sleep(10)
        return self.cursor.fetchall()

    def change_entry_in_database(self, sql, data):
        try_count = 0
        while True:
            try_count += 1
            try:
                self.cursor.execute(sql, data)
                self.conn.commit()
                break
            except sqlite3.OperationalError as e:
                if try_count == 10:
                    logger.warning("Database locked, giving up. ({}/10)".format(try_count))
                    sys.exit(1)
                else:
                    logger.warning("Database locked, waiting 10s for next try. ({}/10) [{}]".format(try_count, e))
                    time.sleep(10)
        return self.cursor.lastrowid

    def bulk_insert_entry_in_database(self, sql, data):
        try_count = 0
        while True:
            try_count += 1
            try:
                self.cursor.executemany(sql, data)
                self.conn.commit()
                break
            except sqlite3.OperationalError:
                if try_count == 10:
                    logger.warning("Database locked, giving up. ({}/10)".format(try_count))
                    sys.exit(1)
                else:
                    logger.warning("Database locked, waiting 10s for next try. ({}/10)".format(try_count))
                    time.sleep(10)
        return True






    def get_files_to_be_encrypted(self):
        sql = '''SELECT id, filename, path FROM files
                 WHERE downloaded = 1
                 AND encrypted = 0'''
        return self.fetchall_from_database(sql)

    def update_filename_enc(self, filename_enc, id):
        sql = '''UPDATE files
                 SET filename_encrypted = ?
                 WHERE id = ?'''
        return self.change_entry_in_database(sql, (filename_enc, id,))

    def update_file_after_encrypt(self, filesize, encrypted_date, md5sum_encrypted, id):
        sql = '''UPDATE files
                 SET encrypted_filesize = ?,
                     encrypted_date = ?,
                     md5sum_encrypted = ?,
                     encrypted = 1
                 WHERE id = ?'''
        return self.change_entry_in_database(sql, (filesize, encrypted_date, md5sum_encrypted, id))

    def get_full_tape(self, label):
        sql = '''SELECT id, label, full FROM tapedevices
                 WHERE label = ?
                 AND full = 1'''
        return self.fetchall_from_database(sql, (label,))

    def get_full_tapes(self):
        sql = '''SELECT label FROM tapedevices
                 WHERE full = 1'''
        return self.fetchall_from_database(sql)

    def get_used_tapes(self, label):
        sql = '''SELECT id, label, full FROM tapedevices
                 WHERE label = ?
                 AND full = 0'''
        return self.fetchall_from_database(sql, (label,))

    def write_tape_into_database(self, label):
        sql = '''INSERT OR IGNORE INTO tapedevices (label)
                 VALUES (?)'''
        return self.change_entry_in_database(sql, (label,))



    def get_filecount_by_tapelabel(self, label):
        sql = '''SELECT count(*) FROM files
                 WHERE tape = ?'''
        return self.fetchall_from_database(sql, (label,))

    def get_files_by_tapelabel(self, label):
        sql = '''SELECT id,
                        filename,
                        path,
                        filesize,
                        md5sum_encrypted,
                        filename_encrypted,
                        tapeposition
                 FROM files
                 WHERE tape = ?
                 ORDER BY tapeposition'''
        return self.fetchall_from_database(sql, (label,))

    def mark_tape_as_full(self, label, dt):
        count = self.get_filecount_by_tapelabel(label)[0][0]
        sql = '''UPDATE tapedevices
                 SET full_date = ?,
                     files_count = ?,
                     full = 1
                 WHERE label = ?'''
        return self.change_entry_in_database(sql, (dt, count, label))

    def update_file_after_write(self, dt, label, did, tape_position):
        sql = '''UPDATE files
                 SET written_date = ?,
                     tape = ?,
                     written = 1,
                     tapeposition = ?
                 WHERE id = ?'''
        return self.change_entry_in_database(sql, (dt, label, tape_position, did))

    def list_duplicates(self):
        sql = '''SELECT files.path,
                        files.mtime,
                        alternative_file_names.path,
                        files.filesize
                 FROM files, alternative_file_names
                 WHERE files.id = alternative_file_names.files_id'''
        return self.fetchall_from_database(sql)

    def filename_encrypted_already_used(self, filename_encrypted):
        sql = '''SELECT id FROM files
                 WHERE filename_encrypted = ?'''
        if len(self.fetchall_from_database(sql, (filename_encrypted,))) > 0:
            return True
        else:
            return False

    def dump_filenames_to_for_tapes(self, label):
        sql = '''SELECT id, path, filename_encrypted FROM files
                 WHERE tape = ?'''
        return self.fetchall_from_database(sql, (label,))

    def get_minimum_verified_count(self):
        sql = 'SELECT MIN(verified_count) FROM files LIMIT 1'
        return self.fetchall_from_database(sql)

    def get_ids_by_verified_count(self, verified_count):
        sql = '''SELECT id, filename, filename_encrypted, tape FROM files
                 WHERE tape NOT NULL
                 AND verified_count = ?'''
        return self.fetchall_from_database(sql, (verified_count,))

    def set_file_alternative_deleted(self, fileid):
        sql = '''UPDATE alternative_file_names
                 SET deleted = 1
                 WHERE id = ?'''
        return self.change_entry_in_database(sql, (fileid,))

    def get_file_count(self):
        sql = '''SELECT (SELECT count(*) FROM files WHERE deleted != 1)
                 + (SELECT count(*) FROM alternative_file_names WHERE deleted != 1)
                 AS total_rows'''
        return self.fetchall_from_database(sql)[0][0]

    def get_min_file_size(self):
        sql = '''SELECT MIN(filesize) FROM files WHERE deleted != 1'''
        return self.fetchall_from_database(sql)[0][0]

    def get_max_file_size(self):
        sql = '''SELECT MAX(filesize) FROM files WHERE deleted != 1'''
        return self.fetchall_from_database(sql)[0][0]

    def get_total_file_size(self):
        sql = '''SELECT SUM(filesize) FROM files WHERE deleted != 1'''
        return self.fetchall_from_database(sql)[0][0]

    def get_end_of_data_by_tape(self, tag):
        sql = '''SELECT end_of_data from tapedevices WHERE label = ?'''
        return self.fetchall_from_database(sql, (tag,))[0][0]

    def update_tape_end_position(self, label, tape_position):
        count = self.get_filecount_by_tapelabel(label)[0][0]
        sql = '''UPDATE tapedevices
                 SET end_of_data = ?
                 WHERE label = ?'''
        return self.change_entry_in_database(sql, (tape_position, label))

    def add_restore_job(self):
        date = int(time.time())
        sql = '''INSERT INTO restore_job (startdate)
                 VALUES (?)'''
        return self.change_entry_in_database(sql, (date,))

    def add_restore_job_files(self, jobid, fileids):
        sql = '''INSERT OR IGNORE INTO restore_job_files_map (files_id, restore_job_id)
                 VALUES (?,?)'''
        return self.bulk_insert_entry_in_database(sql, ((id, jobid) for id in fileids))

    def delete_restore_job(self, id):
        sql = '''DELETE FROM restore_job
                 WHERE id = ?'''
        self.change_entry_in_database(sql, (id,))

    def get_restore_job_stats_total(self, jobid=None):
        sql = '''SELECT a.id,
                        a.startdate,
                        a.finished,
                        COUNT(b.files_id),
                        SUM(c.filesize),
                        COUNT(DISTINCT c.tape)
                 FROM restore_job a
                 LEFT JOIN restore_job_files_map b ON b.restore_job_id = a.id
                 LEFT JOIN files c ON c.id = b.files_id
                 WHERE {}
                 GROUP BY a.id {}'''
        if jobid is not None:
            sql = sql.format(f'a.id = {jobid}', '')
        else:
            sql = sql.format('true', 'ORDER BY a.id DESC LIMIT 1')
        return self.fetchall_from_database(sql)

    def get_restore_job_stats_remaining(self, jobid=None):
        sql = '''SELECT a.id,
                        a.startdate,
                        a.finished,
                        count(b.files_id),
                        sum(c.filesize),
                        count(DISTINCT c.tape)
                 FROM restore_job a
                 LEFT JOIN restore_job_files_map b ON b.restore_job_id = a.id
                 LEFT JOIN files c ON c.id = b.files_id
                 WHERE b.restored = 0 AND {}
                 GROUP BY a.id {}'''
        if jobid is not None:
            sql = sql.format(f'a.id = {jobid}', '')
        else:
            sql = sql.format('true', 'ORDER BY a.id DESC LIMIT 1')
        return self.fetchall_from_database(sql)

    def set_restore_job_finished(self, jobid):
        sql = '''UPDATE restore_job
                 SET finished = strftime('%s', 'now')
                 WHERE id = ?'''
        return self.change_entry_in_database(sql, (jobid,))

    def get_latest_restore_job(self):
        sql = 'SELECT id, startdate FROM restore_job ORDER BY id DESC LIMIT 1'
        res = self.fetchall_from_database(sql)
        if res:
            return res[0][0], res[0][1]
        else:
            return None, None

    def get_restore_job_files(self, jobid, tapes=None, restored=False):
        if tapes:
            tape_sql = ' OR '.join(['tape = ?'] * len(tapes))
            args = (jobid, *tapes)
        else:
            tape_sql = 'true'
            args = (jobid,)

        # only print non-restored files if restored is False
        restored_sql = 'AND a.restored = 0' if not restored else ''

        sql = f'''SELECT files_id,
                         filename,
                         path,
                         filesize,
                         tape,
                         restored,
                         filename_encrypted
                  FROM restore_job_files_map a
                  LEFT JOIN files b ON b.id = a.files_id
                  WHERE restore_job_id = ? AND ({tape_sql}) {restored_sql}'''

        return self.fetchall_from_database(sql, args)

    def get_files_like(self, likes=[], tape=None, items=[], written=False):
        return self.get_files_by_path(likes, tape, items, file_compare='path like ?', written=written)

    def get_files_by_path(self, files=[], tape=None, items=[], file_compare='path = ?', written=False):
        if files:
            where_files = ' or '.join([file_compare] * len(files))
        else:
            where_files = 'true'

        if items:
            items_sql = ','.join(items)
        else:
            items_sql = '*'

        sql = f'SELECT {items_sql} FROM files WHERE ({where_files})'

        if tape is not None:
            sql += ' AND tape = ?'
            files += [tape]

        if written:
            sql += ' AND written=1'

        return self.fetchall_from_database(sql, files)

    def set_file_restored(self, restore_id, file_id):
        sql = '''UPDATE restore_job_files_map
                 SET restored = 1
                 WHERE restore_job_id = ? AND files_id = ?'''
        return self.change_entry_in_database(sql, (restore_id, file_id))

    def revert_written_to_tape_by_label(self, label):
        # Use with caution! This will remove written and tape dependencies from all files attached to given label
        sql = '''UPDATE files
                 SET written = 0,
                 written_date = NULL,
                 tape = NULL
                 WHERE tape = ?'''
        return self.change_entry_in_database(sql, (label,))
