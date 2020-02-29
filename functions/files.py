import logging
from tabulate import tabulate
from datetime import datetime

logger = logging.getLogger()

class Files:
    def __init__(self, config, database, tapelibrary, tools):
        self.config = config
        self.database = database
        self.tapelibrary = tapelibrary
        self.tools = tools

    def list(self, short):
        table = []
        files = self.database.get_all_files()
        if not short:
            for i in files:
                table.append([
                    i[0],
                    i[1],
                    i[2],
                    i[3],
                    datetime.utcfromtimestamp(int(i[4])).strftime('%Y-%m-%d %H:%M:%S') if i[4] is not None else "",
                    i[5],
                    i[6],
                    i[7],
                    i[8],
                    i[9],
                    datetime.utcfromtimestamp(int(i[10])).strftime('%Y-%m-%d %H:%M:%S') if i[10] is not None else "",
                    datetime.utcfromtimestamp(int(i[11])).strftime('%Y-%m-%d %H:%M:%S') if i[11] is not None else "",
                    datetime.utcfromtimestamp(int(i[12])).strftime('%Y-%m-%d %H:%M:%S') if i[12] is not None else "",
                    i[13],
                    i[14],
                    i[15],
                    i[16],
                    datetime.utcfromtimestamp(int(i[17])).strftime('%Y-%m-%d %H:%M:%S') if i[17] is not None else "",
                    i[18]
                ])
            print(tabulate(table, headers=[
                'Id',
                'Filename',
                'Path',
                'Filename Encrypted',
                'Modified Date',
                'Filesize'
                'Filesize Encrypted',
                'md5sum',
                'md5sum Encrypted',
                'Tape',
                'Downloaded Date',
                'Encrypted Date',
                'Written Date',
                'Downloaded',
                'Encrypted',
                'Written',
                'Verified Count',
                'Verified Last Date',
                'Deleted'
            ], tablefmt='grid'))
        else:
            for i in files:
                if i[18] == 0:
                    table.append([
                        i[0],
                        i[1],
                        datetime.utcfromtimestamp(int(i[4])).strftime('%Y-%m-%d %H:%M:%S') if i[4] is not None else "",
                        i[5],
                        i[9]
                    ])
            print(tabulate(table, headers=[
                'Id',
                'Path',
                'Modified Date',
                'Filesize'
                'Tape'
            ], tablefmt='grid'))



    def duplicate(self):
        table = []
        dup = self.database.list_duplicates()
        for i in dup:
            table.append([
                i[0],
                datetime.utcfromtimestamp(int(i[1])).strftime('%Y-%m-%d %H:%M:%S'),
                i[2],
                i[3]
            ])
        print(tabulate(table, headers=['Original Name', 'Modified Date', 'Second Name', 'Filesize'], tablefmt='grid'))