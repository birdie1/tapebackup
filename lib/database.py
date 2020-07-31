import datetime
import logging
import os
import sqlite3
import sys
import time
from sqlalchemy import create_engine, func, or_, and_
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.serializer import dumps
from sqlalchemy.orm import sessionmaker
from lib.models import Config, File, Tape, RestoreJob, RestoreJobFileMap

logger = logging.getLogger()


def connect(db_path):
    engine = create_engine(f"sqlite:///{db_path}")
    return engine


def create_tables(engine):
    Config.__table__.create(bind=engine, checkfirst=True)
    File.__table__.create(bind=engine, checkfirst=True)
    Tape.__table__.create(bind=engine, checkfirst=True)
    RestoreJob.__table__.create(bind=engine, checkfirst=True)
    RestoreJobFileMap.__table__.create(bind=engine, checkfirst=True)


def create_session(engine):
    session = sessionmaker(bind=engine)
    return session()


def db_model_version_need_update(engine, session, db_version):
    logger.debug("Check if database need upgrade")
    if engine.dialect.has_table(engine, 'config'):
        version = session.query(Config).filter(Config.name == 'version').first()
        if int(version.value) != db_version:
            logger.error(f"Database need manual upgrade, please run './main.py db upgrade' to upgrade from "
                         f"{version.value} to {db_version}")
            return True
        else:
            return False
    else:
        logger.error(f"Database need migration from previous state, please run './main.py db migrate'")
        return True


def init(db_path, db_version):
    engine = connect(db_path)
    session = create_session(engine)
    if not os.path.exists(db_path):
        logger.info("Creating database")
        create_tables(engine)
        insert_or_update_db_version(session, db_version)

    if db_model_version_need_update(engine, session, db_version):
        session.close()
        return False

    session.close()
    return engine


def insert_or_update_db_version(session, db_version):
    version = session.query(Config).filter(Config.name == 'version').first()
    if version is None:
        version = Config(name='version')
        session.add(version)

    version.value = db_version
    session.commit()


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


def commit(session):
    try_count = 0
    while True:
        try_count += 1
        try:
            session.commit()
            break
        except OperationalError as error:
            if try_count > 10:
                logger.error(f"Database locked, giving up. ({try_count}/10). Error: {error}")
                logger.error(f"Please run ./main.py db repair to remove stale entries!")
                sys.exit(1)
            else:
                logger.warning(f"Database locked, waiting 5 seconds for next retry ({try_count}/10). Error: {error}")
                time.sleep(5)
        except sqlite3.OperationalError:
            if try_count > 10:
                logger.error(f"Database locked, giving up. ({try_count}/10). Error: {error}")
                logger.error(f"Please run ./main.py db repair to remove stale entries!")
                sys.exit(1)
            else:
                logger.warning(f"Database locked, waiting 5 seconds for next retry ({try_count}/10). Error: {error}")
                time.sleep(5)


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

    commit(session)


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

    commit(session)
    return file


def delete_broken_file(session, file):
    session.delete(file)
    commit(session)


def get_all_files(session):
    return session.query(File).all()


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
    commit(session)


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
    commit(session)


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
    commit(session)
    return file


def update_file_after_encrypt(session, file, filesize, encrypted_date, md5sum_encrypted):
    file.filesize_encrypted = filesize
    file.encrypted_date = encrypted_date
    file.md5sum_encrypted = md5sum_encrypted
    file.encrypted = True
    commit(session)


def get_full_tapes(session):
    return session.query(Tape).filter(Tape.full.is_(True)).all()


def write_tape_into_database(session, label):
    tape = Tape(label=label)
    session.add(tape)
    commit(session)


def get_end_of_data_by_tape(session, label):
    return session.query(Tape.end_of_data).filter(Tape.label == label).first()


def get_files_by_tapelabel(session, label):
    return session.query(File).filter(File.tape.label == label).all()


def revert_written_to_tape_by_label(session, label):
    # Use with caution! This will remove written and tape dependencies from all files attached to given label
    for file in get_files_by_tapelabel(session, label):
        file.written = False
        file.written_date = None
        file.tape_id = None
        commit(session)


def update_file_after_write(session, file, dt, label, tape_position=None):
    tape = session.query(Tape).filter(Tape.label == label).first()
    file.written_date = dt
    file.tape_id = tape.id
    file.written = True
    file.tapeposition = tape_position
    commit(session)


def update_tape_end_position(session, label, tape_position):
    tape = session.query(Tape).filter(Tape.label == label).first()
    tape.end_of_data = tape_position
    commit(session)


def mark_tape_as_full(session, label, dt, count):
    tape = session.query(Tape).filter(Tape.label == label).first()
    tape.full_date = dt
    tape.full = True
    tape.files_count = count
    commit(session)


def get_full_tape(session, label):
    return session.query(Tape).filter(Tape.label == label, Tape.full.is_(True)).first()


def add_restore_job(session):
    job = RestoreJob(startdate=datetime.datetime.now())
    session.add(job)
    commit(session)


def add_restore_job_files(session, jobid, fileids):
    for i in fileids:
        job = RestoreJobFileMap(file_id=i, restore_job_id=jobid)
        session.add(job)
    commit(session)


def get_restore_job_files(session, jobid, tapes=None, restored=False):
    filters = ()
    if tapes is not None:
        for tape in tapes:
            filters += (Tape.label == tape,)

    if restored:
        files = session.query(File).join(RestoreJobFileMap).join(Tape).filter(
                    RestoreJobFileMap.restore_job_id == jobid,
                    or_(*filters)
                ).all()
    else:
        files = session.query(File).join(RestoreJobFileMap).join(Tape).filter(
            RestoreJobFileMap.restore_job_id == jobid,
            RestoreJobFileMap.restored == restored,
            or_(*filters)
        ).all()

    return files


def set_file_restored(session, restore_id, file_id):
    job_map = session.query(RestoreJobFileMap).filter(RestoreJobFileMap.restore_job_id, RestoreJobFileMap.file_id).first()
    job_map.restored = True
    commit(session)


def set_restore_job_finished(session, jobid):
    job = session.query(RestoreJob).filter(RestoreJob.id == jobid).first()
    job.finished = datetime.datetime.now()
    commit(session)


def get_latest_restore_job(session):
    return session.query(RestoreJob).order_by(RestoreJob.id.desc()).first()


def delete_restore_job(session, id):
    session.query(RestoreJob).filter(RestoreJob.id == id).delete()
    commit(session)


def get_restore_job_stats_remaining(session, jobid=None):
    if jobid is None:
        jobid = get_latest_restore_job(session)

    job = session.query(
        RestoreJob.id,
        RestoreJob.startdate,
        func.count(RestoreJobFileMap.id),
        func.sum(File.filesize),
        func.count(File.tape_id.distinct())
    ).join(
        RestoreJobFileMap, RestoreJobFileMap.restore_job_id == RestoreJob.id
    ).join(
        File, RestoreJobFileMap.file_id == File.id
    ).filter(RestoreJob.id == jobid, RestoreJobFileMap.restored == False).first()

    return job


def get_restore_job_stats_total(session, jobid=None):
    if jobid is None:
        jobid = get_latest_restore_job(session)

    job = session.query(
        RestoreJob.id,
        RestoreJob.startdate,
        func.count(RestoreJobFileMap.id),
        func.sum(File.filesize),
        func.count(File.tape_id.distinct())
    ).join(
        RestoreJobFileMap, RestoreJobFileMap.restore_job_id == RestoreJob.id
    ).join(
        File, RestoreJobFileMap.file_id == File.id
    ).filter(RestoreJob.id == jobid).first()

    return job


def get_files_like(session, filelist=[], tape=None, written=False):
    tape_filters = ()
    file_filters = ()
    if tape is not None:
        tape_filters += (Tape.label == tape,)

    for file in filelist:
        file_filters += (File.path.contains(file),)

    if written:
        files = session.query(File).join(Tape).filter(
            or_(*tape_filters),
            or_(*file_filters),
            File.written.is_(True)
        ).all()
    else:
        files = session.query(File).join(Tape).filter(
            or_(*tape_filters),
            and_(*file_filters)
        ).all()

    return files
