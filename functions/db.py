import logging
import os
import time

logger = logging.getLogger()


class Db:
    def __init__(self, config, database, tapelibrary, tools, local=False):
        self.config = config
        self.database = database
        self.tapelibrary = tapelibrary
        self.tools = tools
        self.local_files = local
        self.interrupted = False

    def set_interrupted(self):
        self.interrupted = True

    def init(self):
        if self.database.create_tables():
            logger.info("Tables created")

    def repair(self):
        broken_d = self.database.get_broken_db_download_entry()
        for file in broken_d:
            if os.path.isfile("{}/{}".format(self.config['local-data-dir'], file[1])):
                os.remove("{}/{}".format(self.config['local-data-dir'], file[1]))

            logger.info("Fixing Database ID: {}".format(file[0]))
            self.database.delete_broken_db_download_entry(file[0])

        logger.info("Fixed {} messed up download entries".format(len(broken_d)))

        broken_p = self.database.get_broken_db_encrypt_entry()
        for file in broken_p:
            if os.path.isfile("{}/{}".format(self.config['local-enc-dir'], file[1])):
                os.remove("{}/{}".format(self.config['local-enc-dir'], file[1]))

            logger.info("Fixing Database ID: {}".format(file[0]))
            self.database.update_broken_db_encrypt_entry(file[0])

        logger.info("Fixed {} messed up encrypt entries".format(len(broken_p)))

    def fix_timestamp(self):
        fixed = 0
        files = self.database.get_all_files()
        for i in files:
            try:
                int(i[4])
            except TypeError:
                continue
            except ValueError:
                self.database.fix_float_timestamps(i[0], int(float(i[4])))
                fixed += 1

        logger.info("Fix Timestamps: fixed: {}, already ok: {}".format(fixed, len(files) - fixed))

    def status(self):
        tables = self.database.get_tables()

        for i in tables:
            print("")
            print("######### SHOW TABLE {} ##########".format(i))
            self.database.total_rows(i, print_out=True)
            self.database.table_col_info(i, print_out=True)
            self.database.values_in_col(i, print_out=True)

    def backup(self):
        self.database.export('{}/tapebackup-{}.sql'.format(self.config['database-backup-git-path'], int(time.time())))
        ## TODO: Compare to old git and commit if changed
