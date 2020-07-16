import datetime
import sqlite3
import logging
import sys
import time
from lib import database as new_db
from sqlalchemy.exc import IntegrityError
from lib.models import Config, File, Tape, RestoreJob, RestoreJobFileMap
from sqlite3 import Error

logger = logging.getLogger()


class Migrate:
    def __init__(self, config, db_version):
        self.config = config
        self.conn = self.create_connection(config['database'])
        self.cursor = self.conn.cursor()

        self.engine = new_db.connect(f"migrate-new-{config['database']}")
        self.session = new_db.create_session(self.engine)
        new_db.create_tables(self.engine)
        new_db.insert_or_update_db_version(self.session, db_version)

    def create_connection(self, db_file):
        conn = None
        try:
            conn = sqlite3.connect(db_file)
        except Error as e:
            print(e)

        return conn

    def fetchall_from_old_database(self, sql, data=()):
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

    def fetch_tapedevices(self):
        sql = '''SELECT * FROM tapedevices'''
        return self.fetchall_from_old_database(sql)

    def write_tape(self, label, full_date, files_count, end_of_data, full, verified_count, verified_last):
        tape = Tape(
            label=label,
            full_date=full_date,
            files_count=files_count,
            end_of_data=end_of_data,
            full=full,
            verified_count=verified_count,
            verified_last=verified_last
        )
        self.session.add(tape)
        try:
            self.session.commit()
        except IntegrityError:
            logger.warning(f"Tape {label} already exists, ignoring!")
            self.session.rollback()

    def count_old_tapes(self):
        sql = '''SELECT count(*) FROM tapedevices'''
        return self.fetchall_from_old_database(sql)[0][0]

    def count_new_tapes(self):
        return self.session.query(Tape).count()

    def fetch_files(self):
        sql = '''SELECT * FROM files'''
        return self.fetchall_from_old_database(sql)

    def add_files(self, filename, path, filename_encrypted, mtime, filesize, filesize_encrypted, md5sum_file,
                    md5sum_encrypted, tape_label, downloaded_date, encrypted_date, written_date, tapeposition,
                    downloaded, encrypted, written, verified_count, verified_last, deleted):
        tape = self.session.query(Tape).filter(Tape.label == tape_label).first()
        if tape is None:
            tape_id = None
        else:
            tape_id = tape.id
        file = File(
            filename=filename,
            path=path,
            filename_encrypted=filename_encrypted,
            mtime=mtime,
            filesize=filesize,
            filesize_encrypted=filesize_encrypted,
            md5sum_file=md5sum_file,
            md5sum_encrypted=md5sum_encrypted,
            tape=tape_id,
            downloaded_date=downloaded_date,
            encrypted_date=encrypted_date,
            written_date=written_date,
            tapeposition=tapeposition,
            downloaded=downloaded,
            encrypted=encrypted,
            written=written,
            verified_count=verified_count,
            verified_last=verified_last,
            deleted=deleted
        )
        self.session.add(file)

    def commit(self):
        try:
            self.session.commit()
        except IntegrityError:
            logger.warning(f"Some Files already exists, ignoring this batch!")
            self.session.rollback()

    def count_old_files(self):
        sql = '''SELECT count(*) FROM files'''
        return self.fetchall_from_old_database(sql)[0][0]

    def count_new_files(self):
        return self.session.query(File).filter(File.duplicate_id.is_(None)).count()

    def fetch_alternative_files(self):
        sql = '''SELECT * FROM alternative_file_names'''
        return self.fetchall_from_old_database(sql)

    def get_filepath_from_old_db_by_id(self, file_id):
        sql = '''SELECT path FROM files WHERE id = ?'''
        fetch = self.fetchall_from_old_database(sql, (file_id,))
        if len(fetch) > 0:
            return fetch[0][0]
        else:
            return False

    def add_dup_files(self, filename, path, parent_file_id, mtime, deleted):
        old_path = self.get_filepath_from_old_db_by_id(parent_file_id)
        if not old_path:
            logger.warning(f"Parent file not found: parent id: {parent_file_id}, dup file path: {path}")
            return False
        parent = self.session.query(File).filter(File.path == old_path).first()
        file = File(
            filename=filename,
            path=path,
            duplicate_id=parent.id,
            mtime=mtime,
            deleted=deleted
        )
        self.session.add(file)

    def count_old_dup_files(self):
        sql = '''SELECT count(*) FROM alternative_file_names'''
        return self.fetchall_from_old_database(sql)[0][0]

    def count_new_dup_files(self):
        return self.session.query(File).filter(File.duplicate_id.isnot(None)).count()

    def fetch_restore_jobs(self):
        sql = '''SELECT * FROM restore_job'''
        return self.fetchall_from_old_database(sql)

    def write_restore_jobs(self, startdate, finished):
        restore_job = RestoreJob(
            startdate=startdate,
            finished=finished
        )
        self.session.add(restore_job)
        try:
            self.session.commit()
        except IntegrityError:
            logger.warning(f"Restore Job {startdate}-{finished} already exists, ignoring!")
            self.session.rollback()

    def count_old_restore_jobs(self):
        sql = '''SELECT count(*) FROM restore_job'''
        return self.fetchall_from_old_database(sql)[0][0]

    def count_new_restore_jobs(self):
        return self.session.query(RestoreJob).count()

    def fetch_restore_jobs_file_maps(self):
        sql = '''SELECT * FROM restore_job_files_map'''
        return self.fetchall_from_old_database(sql)

    def get_restore_job_from_old_db_by_id(self, restore_job_id):
        sql = '''SELECT startdate, finished FROM restore_job WHERE id = ?'''
        return self.fetchall_from_old_database(sql, (restore_job_id,))[0]

    def add_restore_job_maps(self, restored, files_id, restore_job_id):
        old_path = self.get_filepath_from_old_db_by_id(files_id)
        if not old_path:
            logger.warning(f"File is not found: {files_id}")
            return False
        old_restore_job = self.get_restore_job_from_old_db_by_id(restore_job_id)

        file = self.session.query(File).filter(File.path == old_path).first()
        restore_job = self.session.query(RestoreJob).filter(
            RestoreJob.startdate == datetime.datetime.fromtimestamp(int(old_restore_job[0])),
            RestoreJob.finished == datetime.datetime.fromtimestamp(int(old_restore_job[1]))
        ).first()


        restore_job_map = RestoreJobFileMap(
            restored=restored,
            file_id=file.id,
            restore_job_id=restore_job.id
        )

        self.session.add(restore_job_map)

    def count_old_restore_job_maps(self):
        sql = '''SELECT count(*) FROM restore_job_files_map'''
        return self.fetchall_from_old_database(sql)[0][0]

    def count_new_restore_job_maps(self):
        return self.session.query(RestoreJobFileMap).count()
