import datetime
import logging
import os
import time
import shutil
from lib import database
from lib.migrate import Migrate
logger = logging.getLogger()


class Db:
    def __init__(self, config, engine, tapelibrary, tools, local=False):
        self.config = config
        self.session = database.create_session(engine)
        self.tapelibrary = tapelibrary
        self.tools = tools
        self.local_files = local
        self.interrupted = False

    def set_interrupted(self):
        self.interrupted = True

    def repair(self):
        broken_d = database.get_broken_db_download_entry(self.session)
        for file in broken_d:
            logger.info("Fixing Database ID: {}".format(file.id))
            database.delete_broken_file(self.session, file)

        broken_p = database.get_broken_db_encrypt_entry(self.session)
        for file in broken_p:
            if os.path.isfile(f"{self.config['local-enc-dir']}/{file.filename_encrypted}"):
                os.remove(f"{self.config['local-enc-dir']}/{file.filename_encrypted}")

            logger.info(f"Fixing Database ID: {file.id}")
            database.update_broken_db_encrypt_entry(self.session, file)

        delete_all_missing_files = False
        no2all = False
        delete_m = 0
        files_to_be_written = database.get_files_to_be_written(self.session)
        for file in files_to_be_written:
            if not os.path.isfile(f"{self.config['local-enc-dir']}/{file.filename_encrypted}"):
                delete_this = False
                if not delete_all_missing_files:
                    while True:
                        change = input(f"Encrypted file {file.filename_encrypted} not found, do you want delete entry "
                                       f"from database? (Yes/No/All/No2All)?[Y/n/a/2]: ")
                        if change == "a":
                            delete_all_missing_files = True
                            break
                        elif change == "n":
                            break
                        elif change == "2":
                            no2all = True
                            break
                        else:
                            delete_this = True
                            break
                if delete_this or delete_all_missing_files:
                    logger.warning(f"Encrypted file: {file.id}:{file.filename_encrypted} not found. cleaning up "
                                   f"(delete) database entry")
                    database.delete_broken_file(self.session, file)
                    delete_m += 1

                if no2all:
                    break
        logger.info(f"Fixed {len(broken_d)} messed up download entries (Download not finished)")
        logger.info(f"Fixed {len(broken_p)} messed up encrypt entries (Encryption not finished)")
        logger.info(f"Deleted {delete_m} 'write to tape' entries with missing files")

    def status(self):
        tables = database.get_tables(self.session)

        for table in tables:
            print("")
            print(f"######### SHOW TABLE {table} ##########")
            database.total_rows(self.session, table, print_out=True)
            database.table_col_info(self.session, table, print_out=True)
            database.values_in_col(self.session, table, print_out=True)

    def migrate_tapes(self, migrate):
        logger.info("Writing tapedevices into new database")
        tapedev = migrate.fetch_tapedevices()
        for tape in tapedev:
            id = tape[0]
            label = tape[1]
            if tape[2] is not None:
                full_date = datetime.datetime.fromtimestamp(int(tape[2]))
            else:
                full_date = None
            files_count = tape[3]
            end_of_data = tape[4]
            if tape[5] == 1:
                full = True
            else:
                full = False

            verified_count = tape[6]
            verified_last = tape[7]

            migrate.write_tape(label, full_date, files_count, end_of_data, full, verified_count, verified_last)
        logger.info(f"Writing tape done.")

    def migrate_files(self, migrate):
        logger.info("Writing files into new database")
        files = migrate.fetch_files()
        total = len(files)
        count = 0
        for file in files:
            count += 1
            id = file[0]
            filename = file[1]
            path = file[2]
            filename_encrypted = file[3]
            if file[4] is not None:
                mtime = datetime.datetime.fromtimestamp(int(file[4]))
            else:
                mtime = None
            filesize = file[5]
            filesize_encrypted = file[6]
            md5sum_file = file[7]
            md5sum_encrypted = file[8]
            tape_label = file[9]
            if file[10] is not None:
                downloaded_date = datetime.datetime.fromtimestamp(int(file[10]))
            else:
                downloaded_date = None
            if file[11] is not None:
                encrypted_date = datetime.datetime.fromtimestamp(int(file[11]))
            else:
                encrypted_date = None
            if file[12] is not None:
                written_date = datetime.datetime.fromtimestamp(int(file[12]))
            else:
                written_date = None
            tapeposition = file[13]
            if file[14] == 1:
                downloaded = True
            else:
                downloaded = False
            if file[15] == 1:
                encrypted = True
            else:
                encrypted = False
            if file[16] == 1:
                written = True
            else:
                written = False
            verified_count = file[17]
            verified_last = file[18]
            if file[19] == 1:
                deleted = True
            else:
                deleted = False

            migrate.add_files(filename, path, filename_encrypted, mtime, filesize, filesize_encrypted,
                              md5sum_file, md5sum_encrypted, tape_label, downloaded_date, encrypted_date,
                              written_date, tapeposition, downloaded, encrypted, written, verified_count,
                              verified_last, deleted)
            if count % 10000 == 0:
                migrate.commit()
                logger.info(f"Writing file: {count}/{total} done.")
        migrate.commit()
        logger.info(f"Writing file: {count}/{total} done.")

    def migrate_dup_files(self, migrate):
        logger.info("Writing duplicate files into new database")
        alt_files = migrate.fetch_alternative_files()
        total = len(alt_files)
        count = 0
        for file in alt_files:
            count += 1
            id = file[0]
            filename = file[1]
            path = file[2]
            parent_file_id = file[3]
            if file[4] is not None:
                mtime = datetime.datetime.fromtimestamp(int(file[4]))
            else:
                mtime = None
            if file[5] == 1:
                deleted = True
            else:
                deleted = False

            migrate.add_dup_files(filename, path, parent_file_id, mtime, deleted)
            if count % 10000 == 0:
                migrate.commit()
                logger.info(f"Writing duplicate file: {count}/{total} done.")
        migrate.commit()
        logger.info(f"Writing duplicate file: {count}/{total} done.")

    def migrate_restore_jobs(self, migrate):
        logger.info("Writing restore jobs into new database")
        restore_jobs = migrate.fetch_restore_jobs()
        for jobs in restore_jobs:
            id = jobs[0]
            if jobs[1] is not None:
                startdate = datetime.datetime.fromtimestamp(int(jobs[1]))
            else:
                startdate = None
            if jobs[2] is not None:
                finished = datetime.datetime.fromtimestamp(int(jobs[2]))
            else:
                finished = None
            migrate.write_restore_jobs(startdate, finished)
        logger.info(f"Writing restore jobs done.")

    def migrate_restore_jobs_map(self, migrate):
        logger.info("Writing restore job maps into new database")
        restore_jobs_maps = migrate.fetch_restore_jobs_file_maps()
        total = len(restore_jobs_maps)
        count = 0
        for jmap in restore_jobs_maps:
            count += 1
            id = jmap[0]
            if jmap[1] == 1:
                restored = True
            else:
                restored = False
            files_id = jmap[2]
            restore_job_id = jmap[3]
            migrate.add_restore_job_maps(restored, files_id, restore_job_id)

            if count % 10000 == 0:
                migrate.commit()
                logger.info(f"Writing restore job maps: {count}/{total} done.")
        migrate.commit()
        logger.info(f"Writing restore job maps: {count}/{total} done.")

    def migrate(self, db_version):
        """
        This can migrate database from programm version < 0.3 to current state
        :return:
        """
        logger.info("Starting migration of database")
        logger.info("Creating new database file")
        migrate = Migrate(self.config, db_version)

        self.migrate_tapes(migrate)
        self.migrate_files(migrate)
        self.migrate_dup_files(migrate)
        self.migrate_restore_jobs(migrate)
        self.migrate_restore_jobs_map(migrate)

        logger.info(f"Tapes count: Old DB: {migrate.count_old_tapes()} | New DB: {migrate.count_new_tapes()}")
        logger.info(f"Files count: Old DB: {migrate.count_old_files()} | New DB: {migrate.count_new_files()}")
        logger.info(f"Duplicate Files count: Old DB: {migrate.count_old_dup_files()} | New DB: {migrate.count_new_dup_files()}")
        logger.info(f"Restore jobs count: Old DB: {migrate.count_old_restore_jobs()} | New DB: {migrate.count_new_restore_jobs()}")
        logger.info(f"Restore job maps count: Old DB: {migrate.count_old_restore_job_maps()} | New DB: {migrate.count_new_restore_job_maps()}")

        old = f"old-{int(time.time())}-{self.config['database']}"
        shutil.copy2(self.config['database'], old)
        shutil.copy2(f"migrate-new-{self.config['database']}", self.config['database'])
        logger.info(f"New database '{self.config['database']}' build successful, backup of old database: {old}.")

    def backup(self):
        print("NOT IMPLEMENTED YET!")
        # TODO: Need Rework
        #self.database.export(f"{self.config['database-backup-git-path']}/tapebackup-{int(time.time())}.sql")
        ## TODO: Compare to old git and commit if changed
