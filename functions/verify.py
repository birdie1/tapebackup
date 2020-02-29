import logging
import random

logger = logging.getLogger()

class Verify:
    def __init__(self, config, database, tapelibrary, tools):
        self.config = config
        self.database = database
        self.tapelibrary = tapelibrary
        self.tools = tools


    def file(self, arg, count):
        if arg == "":
            logger.info("Choosing {} files for verification".format(count))
            ids = self.database.get_ids_by_verified_count(
                self.database.get_minimum_verified_count()[0][0]
            )

            tapes, tapes_to_remove = self.tapelibrary.get_tapes_tags_from_library()
            tapes.extend(tapes_to_remove)

            possible_without_tapechange = []
            for id in ids:
                for tape in tapes:
                    if tape in id[2]:
                        possible_without_tapechange.append(id)

            if len(possible_without_tapechange) < count:
                logger.warning("You will need a tapechange for verify {} files".format(count))
                to_verify = possible_without_tapechange
                to_verify.extend(random.choices(ids, k=len(possible_without_tapechange) - count))
            else:
                to_verify = random.choices(possible_without_tapechange, k=count)

            logger.info("Chosen following ids for verify: {}".format([ x[0] for x in to_verify]))





            print(len(ids))
            print(len(possible_without_tapechange))

            print(to_verify)

        print(arg)
        ## TODO: Verify random or by given File


    def tape(self, arg):
        ## TODO: - Verify random or by given Tape
        ##       - Verify filesystem and my be a few files
        ##       - Verify Tapebackup Database file
        pass