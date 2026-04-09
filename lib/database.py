import datetime
import logging
import os
from sqlalchemy import create_engine, func, or_, and_
from sqlalchemy.orm import sessionmaker

from lib.decorators import retry_transaction
from lib.models import Config, File, Tape, RestoreJob, RestoreJobFileMap

logger = logging.getLogger()


def connect(db_path):
    """
    Create database engine
    """
    engine = create_engine(f"sqlite:///{db_path}")
    return engine


def create_tables(engine):
    """
    Create all necessary tables
    """
    Config.__table__.create(bind=engine, checkfirst=True)
    File.__table__.create(bind=engine, checkfirst=True)
    Tape.__table__.create(bind=engine, checkfirst=True)
    RestoreJob.__table__.create(bind=engine, checkfirst=True)
    RestoreJobFileMap.__table__.create(bind=engine, checkfirst=True)


def create_session(engine):
    """
    Create a new database session.
    """
    session = sessionmaker(bind=engine)
    return session()


def db_model_version_need_update(engine, session, db_version):
    """
    Check if the database model needs an upgrade
    """
    connection = engine.connect()
    logger.debug("Check if database need upgrade")
    if engine.dialect.has_table(connection, 'config'):
        version = session.query(Config).filter(Config.name == 'version').first()
        if int(version.value) != db_version:
            logger.error("Database need manual upgrade, please run './main.py db upgrade' to "
                         "upgrade from %s to %s", version.value, db_version)
            return True
        return False
    logger.error("Database need migration from previous state, please run './main.py db migrate'")
    return True


def init(db_path, db_version):
    """
    Initialize a new database
    """
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

@retry_transaction()
def insert_or_update_db_version(session, db_version):
    """
    Insert or upgrade the model version into database
    """
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

@retry_transaction()
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
    """
    Get a file by its md5 checksum
    """
    return session.query(File).filter(File.md5sum_file == md5).first()


@retry_transaction()
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

@retry_transaction()
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

@retry_transaction()
def delete_broken_file(session, file):
    """
    Delete a file, use it to repair stale/malformed db entries
    """
    session.delete(file)
    session.commit()


def get_all_files(session):
    """
    Get all files from database
    """
    return session.query(File).all()


def get_tables(session):
    """
    Get all tables from database
    """
    rows = session.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    return [table[0] for table in rows]


def total_rows(session, table_name, print_out=False):
    """ Returns the total number of rows in the database """
    count = session.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()
    if print_out:
        print(f'Total rows: {count[0]}')
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
    info = session.execute(f'PRAGMA TABLE_INFO({table_name})').fetchall()
    col_dict = {}
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
    """
    Get a list with malformed (typically unfinished) download entries.
    """
    return session.query(File).filter(File.duplicate_id.is_(None), File.downloaded.is_(False)).all()


def get_broken_db_encrypt_entry(session):
    """
    Get a list with malformed (typically unfinished) encrypt entries.
    """
    return session.query(File).filter(File.filename_encrypted.isnot(None), File.encrypted.is_(False)).all()

@retry_transaction()
def update_broken_db_encrypt_entry(session, file):
    """
    Update malformed  (typically unfinished) encrypt entries.
    """
    file.filename_encrypted = None
    session.commit()


def get_files_to_be_written(session):
    """
    Get all files that are ready to be written onto a tape.
    """
    return session.query(File).filter(
        File.downloaded.is_(True),
        File.encrypted.is_(True),
        File.written.is_(False)
    ).all()


def get_not_deleted_files(session, base_path):
    """
    Get all files which are have not the deleted flag (e.g. all files which was last backup time activ on the source)
    """
    return_data = []
    for tut in session.query(File.path).filter(File.deleted.is_(False)).all():
        return_data.append(f"{base_path}/{tut[0]}")
    return return_data

@retry_transaction()
def set_file_deleted(session, filepath, base_path):
    """
    Set the deletion flag on a file
    """
    file = session.query(File).filter(File.path == filepath.replace(base_path, '').strip("/")).first()
    file.deleted = True
    session.commit()
    return file.id


def get_file_count(session):
    """
    Get count of files
    """
    return session.query(File.id).count()


def get_min_file_size(session):
    """
    Get the filesize of the smallest file.
    """
    return session.query(func.min(File.filesize)).first()[0]


def get_max_file_size(session):
    """
    Get the filesize of the largest file.
    """
    return session.query(func.max(File.filesize)).first()[0]


def get_total_file_size(session):
    """
    Get the sum of filesize from all files.
    """
    return session.query(func.sum(File.filesize)).first()[0]


def list_duplicates(session):
    """
    Get all duplicated entries (Same file with another name exists).
    """
    return session.query(File).filter(File.duplicate_id.isnot(None)).all()


def get_files_to_be_encrypted(session):
    """
    Get all files which are downloaded and waiting to be encrypted.
    """
    return session.query(File).filter(File.downloaded.is_(True), File.encrypted.is_(False)).all()


def filename_encrypted_already_used(session, filename_encrypted):
    """
    Check if filename_encrypted is already in use to prevent errors.
    """
    return len(session.query(File).filter(File.filename_encrypted == filename_encrypted).all()) > 0


@retry_transaction()
def update_filename_enc(session, file_id, filename_enc):
    """
    Update file and set encrypted filename
    """
    file = session.query(File).filter(File.id == file_id).first()
    file.filename_encrypted = filename_enc
    session.commit()
    return file


@retry_transaction()
def update_file_after_encrypt(session, file, filesize, encrypted_date, md5sum_encrypted):
    """
    Update file after encryption to set filessize, date and md5sum
    """
    file.filesize_encrypted = filesize
    file.encrypted_date = encrypted_date
    file.md5sum_encrypted = md5sum_encrypted
    file.encrypted = True
    session.commit()


def get_full_tapes(session):
    """
    Get all full tapes
    """
    return session.query(Tape).filter(Tape.full.is_(True)).all()

@retry_transaction()
def write_tape_into_database(session, label):
    """
    Add a new tape to the database
    """
    if session.query(Tape).filter(Tape.label == label).first() is None:
        tape = Tape(label=label)
        session.add(tape)
        session.commit()


def get_end_of_data_by_tape(session, label):
    """
    Get point of end of data on a tape (only tapes which can't be used with ltfs)
    """
    return session.query(Tape.end_of_data).filter(Tape.label == label).first()


def get_files_by_tapelabel(session, label):
    """
    Get all files which are written on a specific tape.
    """
    return session.query(File).join(Tape).filter(Tape.label == label).all()


def get_started_tape(session):
    """
    Get an already written tape which are not full yet to continue writing.
    """
    return session.query(Tape.label).filter(Tape.full.is_(False)).first()

@retry_transaction()
def revert_written_to_tape_by_label(session, label):
    """
    Use with caution! This will remove written and tape dependencies from all files attached to given label
    """
    for file in get_files_by_tapelabel(session, label):
        file.written = False
        file.written_date = None
        file.tape_id = None
        session.commit()

@retry_transaction()
def update_file_after_write(session, file, dt, label, tape_position=None):
    """
    Update file after written to tape with date, tape_id and position
    """
    tape = session.query(Tape).filter(Tape.label == label).first()
    file.written_date = dt
    file.tape_id = tape.id
    file.written = True
    file.tapeposition = tape_position
    session.commit()

@retry_transaction()
def update_tape_end_position(session, label, tape_position):
    """
    Update end of data position on tape (only on tapes which does not support ltfs)
    """
    tape = session.query(Tape).filter(Tape.label == label).first()
    tape.end_of_data = tape_position
    session.commit()

@retry_transaction()
def mark_tape_as_full(session, label, dt, count):
    """
    Update tape and set it as full.
    """
    tape = session.query(Tape).filter(Tape.label == label).first()
    tape.full_date = dt
    tape.full = True
    tape.files_count = count
    session.commit()


def get_full_tape(session, label):
    """
    Get all full tapes.
    """
    return session.query(Tape).filter(Tape.label == label, Tape.full.is_(True)).first()

@retry_transaction()
def add_restore_job(session):
    """
    Add a restore job.
    """
    job = RestoreJob(startdate=datetime.datetime.now())
    session.add(job)
    session.commit()
    return job

@retry_transaction()
def add_restore_job_files(session, jobid, fileids):
    """
    Add files to a restore job.
    """
    for i in fileids:
        job = RestoreJobFileMap(file_id=i, restore_job_id=jobid)
        session.add(job)
    session.commit()


def get_restore_job_files(session, jobid, tapes=None, restored=False):
    """
    Get files for a restore job.
    """
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


@retry_transaction()
def set_file_restored(session, restore_id, file_id):
    """
    Update a restore job file as restored.
    """
    job_map = session.query(RestoreJobFileMap).filter(
        RestoreJobFileMap.restore_job_id == restore_id,
        RestoreJobFileMap.file_id == file_id
    ).first()
    job_map.restored = True
    session.commit()

@retry_transaction()
def set_restore_job_finished(session, jobid):
    """
    Update restore job set it finished.
    """
    job = session.query(RestoreJob).filter(RestoreJob.id == jobid).first()
    job.finished = datetime.datetime.now()
    session.commit()


def get_latest_restore_job(session):
    """
    Get the latest restore job.
    """
    return session.query(RestoreJob).order_by(RestoreJob.id.desc()).first()

@retry_transaction()
def delete_restore_job(session, jobid):
    """
    Delete a restore job.
    """
    session.query(RestoreJob).filter(RestoreJob.id == jobid).delete()
    session.commit()


def get_restore_job_stats_remaining(session, jobid=None, all=False):
    """
    Get remaining statistics from a running restore job.
    """
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
    ).filter(RestoreJobFileMap.restored == False)

    if all:
        return job.all()

    if jobid is None:
        jobid = get_latest_restore_job(session).id
    return job.filter(RestoreJob.id == jobid).first()


def get_restore_job_stats_total(session, jobid=None, all=False):
    """
    Get statistics from all restore jobs.
    """
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
    )

    if all:
        return job.all()

    if jobid is None:
        jobid = get_latest_restore_job(session).id
    return job.filter(RestoreJob.id == jobid).first()


def get_files_like(session, filelist=None, tape=None, written=False):
    """
    Get files with a filter.
    """
    if filelist is None:
        filelist = []
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
