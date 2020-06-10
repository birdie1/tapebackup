import sqlite3
import logging
import sys
import time

from sqlite3 import Error

logger = logging.getLogger()



class Database:
    def __init__(self, config):
        self.config = config
        self.conn = self.create_connection(config['database'])
        self.cursor = self.conn.cursor()

    def create_tables(self):
        sql_files = '''CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY,
                    filename TEXT NOT NULL,
                    path TEXT NOT NULL UNIQUE,
                    filename_encrypted TEXT UNIQUE,
                    mtime TEXT,
                    filesize INT,
                    encrypted_filesize INT,
                    md5sum_file TEXT,
                    md5sum_encrypted TEXT,
                    tape TEXT,
                    downloaded_date TEXT,
                    encrypted_date TEXT,
                    written_date TEXT,
                    tapeposition INT,
                    downloaded INT DEFAULT 0,
                    encrypted INT DEFAULT 0,
                    written INT DEFAULT 0,
                    verified_count INT DEFAULT 0,
                    verified_last TEXT,
                    deleted INT DEFAULT 0
                    );'''

        sql_tapedevice = '''CREATE TABLE IF NOT EXISTS tapedevices (
                    id INTEGER PRIMARY KEY,
                    label TEXT NOT NULL UNIQUE,
                    full_date TEXT,
                    files_count INT DEFAULT 0,
                    end_of_data INT,
                    full INT DEFAULT 0,
                    verified_count INT DEFAULT 0,
                    verified_last TEXT
                    );'''

        sql_alternative_file_names = '''CREATE TABLE IF NOT EXISTS alternative_file_names (
                    id INTEGER PRIMARY KEY,
                    filename TEXT NOT NULL,
                    path TEXT NOT NULL UNIQUE,
                    mtime TEXT,
                    files_id INT NOT NULL,
                    date TEXT ,
                    deleted INT DEFAULT 0
                    );'''

        sql_updates_files = '''CREATE TABLE IF NOT EXISTS updated_files (
                    id INTEGER PRIMARY KEY,
                    files_id INT NOT NULL,
                    filename TEXT NOT NULL,
                    path TEXT NOT NULL,
                    filename_encrypted TEXT UNIQUE,
                    mtime TEXT,
                    filesize INT,
                    encrypted_filesize INT,
                    md5sum_file TEXT,
                    md5sum_encrypted TEXT,
                    tape TEXT,
                    downloaded_date TEXT,
                    encrypted_date TEXT,
                    written_date TEXT,
                    downloaded INT DEFAULT 0,
                    encrypted INT DEFAULT 0,
                    written INT DEFAULT 0,
                    verified_count INT DEFAULT 0,
                    verified_last TEXT,
                    deleted INT DEFAULT 0
                    );'''

        sql_restore_job = '''CREATE TABLE IF NOT EXISTS restore_job (
                    id INTEGER PRIMARY KEY,
                    startdate TEXT NOT NULL,
                    finished TEXT DEFAULT NULL
                    );'''

        sql_restore_job_files = '''CREATE TABLE IF NOT EXISTS restore_job_files_map (
                    id INTEGER PRIMARY KEY,
                    restored INT DEFAULT 0,
                    files_id INTEGER NOT NULL,
                    restore_job_id INTEGER NOT NULL,
                    FOREIGN KEY (files_id) REFERENCES files (id),
                    FOREIGN KEY (restore_job_id) REFERENCES restore_job (id)
                    );'''

        try:
            self.cursor.execute(sql_files)
            self.cursor.execute(sql_tapedevice)
            self.cursor.execute(sql_alternative_file_names)
            self.cursor.execute(sql_updates_files)
            self.cursor.execute(sql_restore_job)
            self.cursor.execute(sql_restore_job_files)
        except Error as e:
            logger.error("Create tables failed:".format(e))
            return False
        return True

    def create_connection(self, db_file):
        conn = None
        try:
            conn = sqlite3.connect(db_file)
        except Error as e:
            print(e)

        return conn

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

    def export(self, filename):
        with open(filename, 'w') as f:
            for line in self.conn.iterdump():
                f.write('{}\n'.format(line))

    def get_tables(self):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        rows = self.cursor.fetchall()
        tables = []
        for table in rows:
            tables.append(table[0])
        return tables

    def total_rows(self, table_name, print_out=False):
        """ Returns the total number of rows in the database """
        self.cursor.execute('SELECT COUNT(*) FROM {}'.format(table_name))
        count = self.cursor.fetchall()
        if print_out:
            print('\nTotal rows: {}'.format(count[0][0]))
        return count[0][0]

    def table_col_info(self, table_name, print_out=False):
        """ Returns a list of tuples with column informations:
        (id, name, type, notnull, default_value, primary_key)
        """
        self.cursor.execute('PRAGMA TABLE_INFO({})'.format(table_name))
        info = self.cursor.fetchall()

        if print_out:
            print("\nColumn Info:\nID, Name, Type, NotNull, DefaultVal, PrimaryKey")
            for col in info:
                print(col)
        return info

    def values_in_col(self, table_name, print_out=True):
        """ Returns a dictionary with columns as keys
        and the number of not-null entries as associated values.
        """
        self.cursor.execute('PRAGMA TABLE_INFO({})'.format(table_name))
        info = self.cursor.fetchall()
        col_dict = dict()
        for col in info:
            col_dict[col[1]] = 0
        for col in col_dict:
            self.cursor.execute('SELECT ({0}) FROM {1} '
                           'WHERE {0} IS NOT NULL'.format(col, table_name))
            # In my case this approach resulted in a
            # better performance than using COUNT
            number_rows = len(self.cursor.fetchall())
            col_dict[col] = number_rows
        if print_out:
            print("\nNumber of entries per column:")
            for i in col_dict.items():
                print('{}: {}'.format(i[0], i[1]))
        return col_dict

    def get_broken_db_download_entry(self):
        sql = ''' SELECT id, path FROM files
                    WHERE downloaded=0
                    '''
        return self.fetchall_from_database(sql)

    def delete_broken_db_entry(self, id):
        sql = ''' DELETE from files
                      WHERE id = ?'''
        self.change_entry_in_database(sql, (id,))

    def get_broken_db_encrypt_entry(self):
        sql = ''' SELECT id, filename_encrypted FROM files
                    WHERE filename_encrypted IS NOT NULL
                    and encrypted=0
                    '''
        return self.fetchall_from_database(sql)

    def get_all_files(self):
        sql = ''' SELECT * FROM files
                    '''
        return self.fetchall_from_database(sql)

    def fix_float_timestamps(self, id, new_timestamp):
        sql = ''' UPDATE files
                    SET mtime = ?
                    WHERE id = ?
                    '''
        return self.change_entry_in_database(sql, (new_timestamp, id,))

    def update_broken_db_encrypt_entry(self, id):
        sql = ''' UPDATE files
                     SET filename_encrypted = NULL
                     WHERE id = ?'''
        self.change_entry_in_database(sql, (id,))

    def check_if_file_exists_by_path(self, relpath):
        """
        Check if filename known in database
        :param relpath:
        :param filename:
        :return: inserted file id
        """
        rows_files = self.fetchall_from_database("SELECT * FROM files WHERE path=?", (relpath,))
        rows_alt_files = self.fetchall_from_database("SELECT * FROM alternative_file_names WHERE path=?", (relpath,))

        if len(rows_files) == 0 and len(rows_alt_files) == 0:
            return False
        else:
            return True

    def insert_file(self, filename, relpath):
        """
        Create a new file entry
        :param file:
        :return: inserted file id
        """
        sql = ''' INSERT INTO files(filename,path)
                  VALUES(?,?) '''
        return self.change_entry_in_database(sql, (filename, relpath,))

    def get_files_by_md5(self, md5):
        sql = ''' SELECT id FROM files
                    WHERE md5sum_file=?
                    '''
        return self.fetchall_from_database(sql, (md5,))

    def insert_alternative_file_names(self, filename, path, duplicate_id, downloaded_date):
        sql = ''' INSERT INTO alternative_file_names (filename,path,files_id,date)
                  VALUES(?,?,?,?) '''
        return self.change_entry_in_database(sql, (filename, path, duplicate_id, downloaded_date,))

    def update_file_after_download(self, filesize, mtime, downloaded_date, md5, downloaded, id):
        sql = ''' UPDATE files
                  SET filesize = ?,
                      mtime = ?,
                      downloaded_date = ?,
                      md5sum_file = ?,
                      downloaded = ?
                  WHERE id = ?'''
        return self.change_entry_in_database(sql, (filesize, mtime, downloaded_date, md5, downloaded, id,))

    def get_files_to_be_encrypted(self):
        sql = ''' SELECT id, filename, path FROM files
                WHERE downloaded=1
                AND encrypted = 0
                '''
        return self.fetchall_from_database(sql)

    def update_filename_enc(self, filename_enc, id):
        sql = ''' UPDATE files
                  SET filename_encrypted = ?
                  WHERE id = ?'''
        return self.change_entry_in_database(sql, (filename_enc, id,))

    def update_file_after_encrypt(self, filesize, encrypted_date, md5sum_encrypted, id):
        sql = ''' UPDATE files
                      SET encrypted_filesize = ?,
                          encrypted_date = ?,
                          md5sum_encrypted = ?,
                          encrypted = 1
                      WHERE id = ?'''
        return self.change_entry_in_database(sql, (filesize, encrypted_date, md5sum_encrypted, id,))

    def get_full_tape(self, label):
        sql = ''' SELECT id, label, full FROM tapedevices
                WHERE label=?
                AND full=1
                '''
        return self.fetchall_from_database(sql, (label,))

    def get_full_tapes(self):
        sql = ''' SELECT label FROM tapedevices
                WHERE full=1
                '''
        return self.fetchall_from_database(sql)

    def get_used_tapes(self, label):
        sql = ''' SELECT id, label, full FROM tapedevices
                WHERE label=?
                AND full=0
                '''
        return self.fetchall_from_database(sql, (label,))

    def write_tape_into_database(self, label):
        sql = ''' INSERT OR IGNORE INTO tapedevices (label)
                          VALUES(?) '''
        return self.change_entry_in_database(sql, (label,))

    def get_files_to_be_written(self):
        sql = ''' SELECT id, filename_encrypted, filename, encrypted_filesize FROM files
                    WHERE downloaded=1
                    AND encrypted=1
                    AND written=0
                    '''
        return self.fetchall_from_database(sql)

    def get_filecount_by_tapelabel(self, label):
        sql = ''' SELECT count(*) FROM files
                    WHERE tape = ?
                    '''
        return self.fetchall_from_database(sql, (label,))

    def get_files_by_tapelabel(self, label):
        sql = ''' SELECT id, filename_encrypted, md5sum_encrypted, filename FROM files
                    WHERE tape = ?
                    '''
        return self.fetchall_from_database(sql, (label,))

    def mark_tape_as_full(self, label, dt):
        count = self.get_filecount_by_tapelabel(label)[0][0]
        sql = ''' UPDATE tapedevices
                  SET full_date = ?,
                      files_count = ?,
                      full = 1
                  WHERE label = ? '''
        return self.change_entry_in_database(sql, (dt, count, label,))

    def update_file_after_write(self, dt, label, did, tape_position):
        sql = ''' UPDATE files
                          SET written_date = ?,
                              tape = ?,
                              written = 1,
                              tapeposition = ?
                          WHERE id = ?'''
        return self.change_entry_in_database(sql, (dt, label, tape_position, did,))

    def list_duplicates(self):
        sql = ''' SELECT files.path, files.mtime, alternative_file_names.path, files.filesize
                            FROM files, alternative_file_names
                            WHERE files.id = alternative_file_names.files_id
                            '''
        return self.fetchall_from_database(sql)

    def filename_encrypted_already_used(self, filename_encrypted):
        sql = ''' SELECT id FROM files
                  WHERE filename_encrypted = ?
                  '''
        if len(self.fetchall_from_database(sql, (filename_encrypted,))) > 0:
            return True
        else:
            return False

    def dump_filenames_to_for_tapes(self, label):
        sql = ''' SELECT id, path, filename_encrypted FROM files
                            WHERE tape = ?
                            '''
        return self.fetchall_from_database(sql, (label,))

    def get_minimum_verified_count(self):
        sql = ''' SELECT MIN(verified_count) FROM files
                            LIMIT 1
                            '''
        return self.fetchall_from_database(sql)

    def get_ids_by_verified_count(self, verified_count):
        sql = ''' SELECT id, filename, filename_encrypted, tape FROM files
                            WHERE tape NOT NULL
                            AND verified_count = ?
                            '''
        return self.fetchall_from_database(sql, (verified_count,))

    def get_not_deleted_files(self):
        sql = ''' SELECT id, path FROM files
                            WHERE deleted != 1
                            '''
        return self.fetchall_from_database(sql)

    def get_not_deleted_alternative_files(self):
        sql = ''' SELECT id, path FROM alternative_file_names
                            WHERE deleted != 1
                            '''
        return self.fetchall_from_database(sql)

    def set_file_deleted(self, fileid):
        sql = ''' UPDATE files
                              SET deleted = 1
                              WHERE id = ?'''
        return self.change_entry_in_database(sql, (fileid,))

    def set_file_alternative_deleted(self, fileid):
        sql = ''' UPDATE alternative_file_names
                              SET deleted = 1
                              WHERE id = ?'''
        return self.change_entry_in_database(sql, (fileid,))

    def get_file_count(self):
        sql = ''' select ( select count(*) from files WHERE deleted != 1)
                            + ( select count(*) from alternative_file_names WHERE deleted != 1)
                            as total_rows
                            '''
        return self.fetchall_from_database(sql)[0][0]

    def get_min_file_size(self):
        sql = ''' select MIN(filesize) from files WHERE deleted != 1'''
        return self.fetchall_from_database(sql)[0][0]

    def get_max_file_size(self):
        sql = ''' select MAX(filesize) from files WHERE deleted != 1'''
        return self.fetchall_from_database(sql)[0][0]

    def get_total_file_size(self):
        sql = ''' select SUM(filesize) from files WHERE deleted != 1'''
        return self.fetchall_from_database(sql)[0][0]

    def get_end_of_data_by_tape(self, tag):
        sql = ''' SELECT end_of_data from tapedevices WHERE label = ?'''
        return self.fetchall_from_database(sql, (tag,))[0][0]

    def update_tape_end_position(self, label, tape_position):
        count = self.get_filecount_by_tapelabel(label)[0][0]
        sql = ''' UPDATE tapedevices
                  SET end_of_data = ?
                  WHERE label = ? '''
        return self.change_entry_in_database(sql, (tape_position, label,))

    def add_restore_job(self):
        date = int(time.time())
        sql = ''' INSERT INTO restore_job (startdate)
                          VALUES(?) '''
        return self.change_entry_in_database(sql, (date,))

    def add_restore_job_files(self, jobid, fileids):
        sql = ''' INSERT INTO restore_job_files_map (files_id, restore_job_id)
                          VALUES(?,?) '''
        return self.bulk_insert_entry_in_database(sql, [(id, jobid) for id in fileids])

    def get_restore_job_stats_total(self, jobid):
        sql = ''' SELECT a.id,a.startdate,a.finished,count(b.files_id),sum(c.filesize), count(DISTINCT c.tape)
                        from restore_job a
                        left join restore_job_files_map b on b.restore_job_id = a.id
                        left join files c on c.id = b.files_id
                        where {}
                        group by a.id {};'''
        if jobid is not None:
            sql = sql.format(f"a.id={jobid}", "")
        else:
            sql = sql.format("true", "ORDER BY a.id DESC LIMIT 1")
        return self.fetchall_from_database(sql)

    def get_restore_job_stats_remaining(self, jobid):
        sql = ''' SELECT a.id,a.startdate,a.finished,count(b.files_id),sum(c.filesize), count(DISTINCT c.tape)
                        from restore_job a
                        left join restore_job_files_map b on b.restore_job_id = a.id
                        left join files c on c.id = b.files_id
                        where b.restored=0 AND {}
                        group by a.id {};'''
        if jobid is not None:
            sql = sql.format(f"a.id={jobid}", "")
        else:
            sql = sql.format("true", "ORDER BY a.id DESC LIMIT 1")
        return self.fetchall_from_database(sql)

    def get_files_like(self, likes=[], tape=None):
        if likes:
            like_sql = " or ".join(["path like ?"] * len(likes))
        else:
            like_sql = "true"
        sql = f"SELECT * FROM files WHERE ({like_sql})"
        if tape is not None:
            sql += " and tape=?"
            likes += [tape]
        return self.fetchall_from_database(sql, likes)
