import logging
import os
from lib import database
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

    def backup(self):
        print("NOT IMPLEMENTED YET!")
        # TODO: Need Rework
        #self.database.export(f"{self.config['database-backup-git-path']}/tapebackup-{int(time.time())}.sql")
        ## TODO: Compare to old git and commit if changed
