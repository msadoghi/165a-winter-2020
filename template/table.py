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
        for index in range(2 * (self.num_columns + 4)):
            base_page = []
            base_page.append(Page())
            self.page_range.append(base_page)
        pass

    def __merge__(self):
        pass

    def __add_physical_page__(self):
        for page_index in range(self.page_range + 4):
            self.page_range[page_index].append(Page()) #add a page at the current column index

    def __insert__(self, columns):
        for column_index in range(self.num_columns + 4):
            for page_index in range(len(self.page_range[column_index])) #Go through every page and check if full, try to write
                slot_index = self.page_range[column_index][0].write(columns[column_index]) #write and return written location
                if slot_index == -1: #if error, need to add extra page to each base page
                    self.__add_physical_page__()
                    continue
                else:
                    self.page_directory[columns[RID_COLUMN]] = (page_index, slot_index) #on successful write, store to page directory
                pass