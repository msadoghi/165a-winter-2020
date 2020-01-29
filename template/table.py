from page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        # TODO: implement page direcetory with page ranges

        self.page_range = []
        #populate page range with base pages, which is a list of physical pages
        for index in range(self.num_columns + 4):
            base_page = []
            base_page.append(Page())
            self.page_range.append(base_page)
        pass

    def __merge__(self):
        pass

    def add_to_base_page(self, page_index):
        self.page_range[index].append(Page())

    def insert(self, columns):
        for index in range(self.num_columns + 4):
            if self.page_range[index][0].write(columns[index]) == -1:
                ##TODO: implement add_to_base_page
                self.add_to_base_page(index)
