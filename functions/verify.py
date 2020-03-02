import logging
import random
import os
import subprocess
import sys
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
                    if tape in id[3]:
                        possible_without_tapechange.append(id)

            if len(possible_without_tapechange) < count:
                logger.warning("You will need a tapechange for verify {} files".format(count))
                to_verify = possible_without_tapechange
                to_verify.extend(random.choices(ids, k=len(possible_without_tapechange) - count))
            else:
                to_verify = random.choices(possible_without_tapechange, k=count)


            ###### FOR DEBUGGING ONLY
            to_verify.extend(random.choices(ids, k=count))
            ###### FOR DEBUGGING ONLY


            logger.info("Chosen following ids for verify: {}".format([x[0] for x in to_verify]))

            needed_tapes = {}
            for i in to_verify:
                try:
                    needed_tapes[i[3]]['count'] += 1
                except KeyError:
                    needed_tapes[i[3]] = {}
                    needed_tapes[i[3]]['count'] = 1
                    if i[3] in tapes:
                        needed_tapes[i[3]]['inside_lib'] = True
                    else:
                        needed_tapes[i[3]]['inside_lib'] = False




            print(needed_tapes)

            sys.exit(0)

            for i in to_verify:
                filename = i[1]
                filename_enc = i[2]
                tape = i[3]

                self.tapelibrary.load(tape)
                self.tapelibrary.ltfs()

                command = ['openssl', 'enc', '-d', '-aes-256-cbc', '-pbkdf2', '-iter', '100000', '-in',
                           os.path.abspath('{}/{}'.format(self.config['local-tape-mount-dir'], filename_enc)),
                           '-out',
                           os.path.abspath('{}/{}'.format(self.config['local-verify-dir'], filename)),
                           '-k',
                           self.config['enc-key']]

                openssl = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setpgrp)
                child_process_pid = openssl.pid

                if len(openssl.stderr.readlines()) == 0:
                    print('BLA')

                break



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