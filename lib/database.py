import sqlite3
import logging
import sys
import time
from sqlalchemy import create_engine, func
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


def get_not_deleted_files(session):
    return session.query(File).filter(File.deleted.is_(False)).all()


def set_file_deleted(session, file):
    file.deleted = True
    session.commit()


def get_file_count(session):
    return session.query(File.id).count()


def get_min_file_size(session):
    return session.query(func.min(File.filesize)).first()[0]


def get_max_file_size(session):
    return session.query(func.max(File.filesize)).first()[0]


def get_total_file_size(session):
    return session.query(func.sum(File.filesize)).first()[0]


def list_duplicates(session):
    return session.query(File).filter(File.duplicate_id.isnot(None)).all()


def get_files_to_be_encrypted(session):
    return session.query(File).filter(File.downloaded.is_(True), File.encrypted.is_(False)).all()


def filename_encrypted_already_used(session, filename_encrypted):
    if len(session.query(File).filter(File.filename_encrypted == filename_encrypted).all()) > 0:
        return True
    else:
        return False


def update_filename_enc(session, id, filename_enc):
    file = session.query(File).filter(File.id == id).first()
    file.filename_encrypted = filename_enc
    session.commit()
    return file


def update_file_after_encrypt(session, file, filesize, encrypted_date, md5sum_encrypted):
    file.encrypted_filesize = filesize
    file.encrypted_date = encrypted_date
    file.md5sum_encrypted = md5sum_encrypted
    file.encrypted = True
    session.commit()


def get_full_tapes(session):
    return session.query(Tape).filter(Tape.full.is_(True)).all()


def write_tape_into_database(session, label):
    tape = Tape(label=label)
    session.add(tape)
    session.commit()


def get_end_of_data_by_tape(session, label):
    return session.query(Tape.end_of_data).filter(Tape.label == label).first()


def get_files_by_tapelabel(session, label):
    return session.query(File).filter(File.tape == label).all()


def revert_written_to_tape_by_label(session, label):
    # Use with caution! This will remove written and tape dependencies from all files attached to given label
    for file in get_files_by_tapelabel(session, label):
        file.written = False
        file.written_date = None
        file.tape = None
        session.commit()


def update_file_after_write(session, file, dt, label, tape_position=None):
    file.written_date = dt
    file.tape = label
    file.written = True
    file.tapeposition = tape_position
    session.commit()


def update_tape_end_position(session, label, tape_position):
    tape = session.query(Tape).filter(Tape.label == label).first()
    tape.end_of_data = tape_position
    session.commit()


def mark_tape_as_full(session, label, dt, count):
    tape = session.query(Tape).filter(Tape.label == label).first()
    tape.full_date = dt
    tape.full = True
    tape.files_count = count
    session.commit()






# TODO from here files.py already changed
def get_files_like(session, likes=[], tape=None, written=False):
    if len(likes):
        where_files = ' or '.join([file_compare] * len(files))
    else:
        where_files = 'true'

def get_files_like_old(self, likes=[], tape=None, items=[], written=False):
    return self.get_files_by_path(likes, tape, items, file_compare='path like ?', written=written)


def get_files_by_path_old(self, files=[], tape=None, items=[], file_compare='path = ?', written=False):
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

# TODO till here files.py already changed


























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












    def get_full_tape(self, label):
        sql = '''SELECT id, label, full FROM tapedevices
                 WHERE label = ?
                 AND full = 1'''
        return self.fetchall_from_database(sql, (label,))

    def get_used_tapes(self, label):
        sql = '''SELECT id, label, full FROM tapedevices
                 WHERE label = ?
                 AND full = 0'''
        return self.fetchall_from_database(sql, (label,))








    def get_minimum_verified_count(self):
        sql = 'SELECT MIN(verified_count) FROM files LIMIT 1'
        return self.fetchall_from_database(sql)

    def get_ids_by_verified_count(self, verified_count):
        sql = '''SELECT id, filename, filename_encrypted, tape FROM files
                 WHERE tape NOT NULL
                 AND verified_count = ?'''
        return self.fetchall_from_database(sql, (verified_count,))





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

    def set_file_restored(self, restore_id, file_id):
        sql = '''UPDATE restore_job_files_map
                 SET restored = 1
                 WHERE restore_job_id = ? AND files_id = ?'''
        return self.change_entry_in_database(sql, (restore_id, file_id))


