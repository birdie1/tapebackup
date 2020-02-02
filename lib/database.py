import sqlite3
import logging

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

        sql_alternative_file_names = '''CREATE TABLE IF NOT EXISTS alternative_file_names (
                    id INTEGER PRIMARY KEY,
                    filename TEXT NOT NULL,
                    path TEXT NOT NULL UNIQUE,
                    files_id INT NOT NULL,
                    date TEXT 
                    );'''

        try:
            self.cursor.execute(sql_files)
            self.cursor.execute(sql_tapedevice)
            self.cursor.execute(sql_alternative_file_names)
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
        self.cursor.execute(sql, data)
        return self.cursor.fetchall()

    def change_entry_in_database(self, sql, data):
        self.cursor.execute(sql, data)
        self.conn.commit()
        return self.cursor.lastrowid



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

    def delete_broken_db_download_entry(self, id):
        sql = ''' DELETE from files
                      WHERE id = ?'''
        self.change_entry_in_database(sql, (id,))

    def get_broken_db_pack_entry(self):
        sql = ''' SELECT id, filename_encrypted FROM files 
                    WHERE filename_encrypted IS NOT NULL
                    and packed=0
                    '''
        return self.fetchall_from_database(sql)

    def update_broken_db_pack_entry(self, id):
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

    def update_file_after_download(self, mtime, downloaded_date, md5, downloaded, id):
        sql = ''' UPDATE files
                  SET mtime = ?,
                      downloaded_date = ?,
                      md5sum_file = ?,
                      downloaded = ?
                  WHERE id = ?'''
        return self.change_entry_in_database(sql, (mtime, downloaded_date, md5, downloaded, id,))
