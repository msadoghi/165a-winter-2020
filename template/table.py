from page import *
from time import time
from index import Index

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
        self.sum = 0
        self.index = Index(self)
        self.page_range = []
        #populate page range with base pages, which is a list of physical pages
        for index in range(2 * (self.num_columns + 4)):
            base_page = []
            base_page.append(Page())
            self.page_range.append(base_page)
        pass

    def __merge__(self):
        pass

    def __add_physical_base_page__(self):
        for page_index in range(self.num_columns + 4):
            self.page_range[page_index].append(Page()) #add a page at the current column index

    def __add_physical_tail_page__(self):
        for page_index in range(self.num_columns + 3, 2 * (self.num_columns + 4)):
            self.page_range[page_index].append(Page()) #add a page at the current column index

    def __insert__(self, columns):
        for column_index in range(self.num_columns + 4):
            # should be a better way of doing this instead of trying to write to each page
            page_index = len((self.page_range[column_index])) -1 
            slot_index = self.page_range[column_index][page_index].write(columns[column_index])
            
            if slot_index == -1: #if latest slot index is -1, need to add another page
                self.__add_physical_base_page__()
                self.page_range[column_index][page_index + 1].write(columns[column_index])
            self.page_directory[columns[RID_COLUMN]] = (page_index, slot_index) #on successful write, store to page directory
            pass


    #TODO: update schema encoding, indirection column of base value using read(?)
    def __update__(self, key, columns):
        RID = self.index.locate(key)
        page_index, slot_index = self.page_directory[RID] 
        tail_RID = config.StartTailRID
        current_i = self.page_range[INDIRECTION_COLUMN][page_index]
        current_s = self.page_range[SCHEMA_ENCODING_COLUMN][page_index]
       
        for column_index in range((self.num_columns + 4) + 1, 2 * (self.num_columns + 4)):



        # get the old record and all the values
        # update the encoding schema
        # get the RID for tail 
        # write to tail





        config.StartTailRID += 1



"""
        for column_index in range((self.num_columns + 4) + 1, 2 * (self.num_columns + 4)): #start at the index after the last base until the last entry 
            for page_index in range(len(self.page_range[column_index])):
                slot_index = self.page_range[column_index][page_index].write(columns[column_index])

            if slot_index == -1:
                self.__add_physical_tail_page__()
            else:
                self.page_directory[columns[RID_COLUMN]] = (page_index, slot_index)
"""
        pass

