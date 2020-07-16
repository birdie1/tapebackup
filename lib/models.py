from sqlalchemy import Column, Integer, String, DateTime, Boolean, UniqueConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
Base = declarative_base()


class Config(Base):
    __tablename__ = 'config'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    value = Column(String)


class File(Base):
    """
    A normal file must have filename and path set, later all fields will be used!
    A duplicate file will have a duplicate_id, filename and path set.
        - It can have set mtime, downloaded_date and deleted.
        - All other fields are only written to the non duplicated file!
    """
    __tablename__ = 'file'

    id = Column(Integer, primary_key=True)
    duplicate_id = Column(Integer, ForeignKey('file.id'))
    filename = Column(String, nullable=False)
    path = Column(String, nullable=False, unique=True)
    filename_encrypted = Column(String, unique=True)
    mtime = Column(DateTime)
    filesize = Column(Integer)
    filesize_encrypted = Column(Integer)
    md5sum_file = Column(String)
    md5sum_encrypted = Column(String)
    tape = Column(Integer, ForeignKey('tape.id'))
    downloaded_date = Column(DateTime)
    encrypted_date = Column(DateTime)
    written_date = Column(DateTime)
    tapeposition = Column(Integer)
    downloaded = Column(Boolean, default=False)
    encrypted = Column(Boolean, default=False)
    written = Column(Boolean, default=False)
    verified_count = Column(Integer, default=0)
    verified_last = Column(DateTime)
    deleted = Column(Boolean, default=False)

    file = relationship("File", remote_side=[id])

    def __repr__(self):
        return f'File object: {self.path}'


class Tape(Base):
    __tablename__ = 'tape'

    id = Column(Integer, primary_key=True)
    label = Column(String, nullable=False, unique=True)
    full_date = Column(DateTime)
    files_count = Column(Integer, default=0)
    end_of_data = Column(Integer)
    full = Column(Boolean, default=False)
    verified_count = Column(Integer, default=0)
    verified_last = Column(DateTime)

    files = relationship("File")

    def __repr__(self):
        return f'Tape object: {self.label}'


class RestoreJob(Base):
    __tablename__ = 'restore_job'

    id = Column(Integer, primary_key=True)
    startdate = Column(DateTime, nullable=False)
    finished = Column(DateTime, default=None)

    def __repr__(self):
        return f'Restore job object: {self.id}'


class RestoreJobFileMap(Base):
    __tablename__ = 'restore_job_file_map'

    id = Column(Integer, primary_key=True)
    restored = Column(Boolean, default=False)
    file_id = Column(Integer, ForeignKey('file.id'), nullable=False)
    restore_job_id = Column(Integer, ForeignKey('restore_job.id'), nullable=False)

    __table_args__ = (UniqueConstraint('file_id', 'restore_job_id'),)

    def __repr__(self):
        return f'Restore job file map object: {self.id}'
