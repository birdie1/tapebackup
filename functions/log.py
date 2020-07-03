import logging
import logging.handlers
import os
import zlib

logger = logging.getLogger()


class Log:
    def __init__(self, config):
        self.config = config
        self.interrupted = False

    def set_interrupted(self):
        self.interrupted = True

    @staticmethod
    def namer(name):
        return name + ".gz"

    @staticmethod
    def rotator(source, dest):
        #print(f'compressing {source} -> {dest}')
        with open(source, "rb") as sf:
            data = sf.read()
            compressed = zlib.compress(data, 9)
            with open(dest, "wb") as df:
                df.write(compressed)
        os.remove(source)

    @staticmethod
    def remove_file_handler():
        for hdlr in logger.handlers:
            if isinstance(hdlr, logging.FileHandler):
                logger.removeHandler(hdlr)

    def rotate(self, logdir, logger_format):
        filenames = next(os.walk(logdir))[2]
        for file in filenames:
            if file.endswith('.log'):
                self.remove_file_handler()

                filehandler = logging.handlers.RotatingFileHandler(f"{logdir}/{file}", maxBytes=10000, backupCount=1000)
                filehandler.rotator = self.rotator
                filehandler.namer = self.namer
                filehandler.setFormatter(
                    logging.Formatter(logger_format)
                )

                logger.addHandler(filehandler)
                logger.info(f"Rotating log {file}")

    @staticmethod
    def remove_debug(logdir):
        filenames = next(os.walk(logdir))[2]
        for file in filenames:
            if file.endswith('.log'):
                logger.info(f"Removing debug entries from {file}")
                with open(f"{logdir}/{file}", "r+") as f:
                    new_f = f.readlines()
                    f.seek(0)
                    for line in new_f:
                        if "[DEBUG  ]" not in line:
                            f.write(line)
                    f.truncate()
