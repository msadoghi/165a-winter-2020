from page import *
from time import time
import config

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
    :param base_RID/tail_RID    #start indexes for records for tail and base ranges
    :param base_range/tail_range #in memory representation of page storage
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.sum = 0

        self.base_RID = config.StartBaseRID
        self.tail_RID = config.StartTailRID
        self.base_range = []
        self.tail_range = []

        #populate page range with base pages, which is a list of physical pages
        for index in range(self.num_columns + config.Offset):
            base_page = []
            base_page.append(Page())
            self.base_range.append(base_page)

        for index in range(self.num_columns + config.Offset):
            tail_page = []
            tail_page.append(Page())
            self.tail_range.append(tail_page)

    def __merge__(self):
        pass

    def __add_physical_base_page__(self):
        for page_index in range(self.num_columns + config.Offset):
            self.base_range[page_index].append(Page()) #add a page at the current column index

    def __add_physical_tail_page__(self):
        for page_index in range(self.num_columns + config.Offset):
            self.tail_range[page_index].append(Page()) #add a page at the current column index

    def __read__(self, RID, query_columns):
        page_index, slot_index = self.page_directory[RID]
        new_rid = self.base_range[INDIRECTION_COLUMN][page_index].read(slot_index) #index into the physical location
        if new_rid != 0:
            page_index, slot_index = self.page_directory[new_rid] #store values from tail record 
        
        # check indir column record
        # update page and slot index based on if there is one or nah
        column_list = []
        key_val = -1
        base_or_tail_range = (self.tail_range if new_rid != 0 else self.base_range)

        # change range to use variables start and end based on base or tail record
        for column_index in range(config.Offset, self.num_columns + config.Offset):
            if column_index == self.key + config.Offset:
                key_val = query_columns[column_index - config.Offset] #subtract offset for the param columns

            if query_columns[column_index - config.Offset] == 1:
                column_val = base_or_tail_range[column_index][page_index].read(slot_index) #index into the physical location
                column_list.append(column_val)

        return Record(RID, key_val, column_list) #return proper record, or -1 on key_val not found

    def __insert__(self, columns):
        for column_index in range(self.num_columns + config.Offset):
            page_index = len((self.base_range[column_index])) - 1 #start at the latest page since everything else is full
            slot_index = self.base_range[column_index][page_index].write(columns[column_index])

            if slot_index == -1: #if latest slot index is -1, need to add another page
                self.__add_physical_base_page__()
                self.base_range[column_index][page_index + 1].write(columns[column_index])
            self.page_directory[columns[RID_COLUMN]] = (page_index, slot_index) #on successful write, store to page directory

    #in place update of the indirection entry. The third flag is a boolean set based on which page range written to
    def __update_indirection__(self, RID, next_RID, write_to_tail):
        page_index, slot_index = self.page_directory[RID] 
        base_or_tail_range = (self.tail_range if write_to_tail else self.base_range)
        base_or_tail_range[INDIRECTION_COLUMN][page_index].inplace_update(slot_index, next_RID)

    def __update_schema_encoding__(self, RID):
        pass
    
    # Set base page entry RID to 0 to invalidate it
    def __delete__ (self, RID):
        page_index, slot_index = self.page_directory[RID]
        self.base_range[RID_COLUMN][page_index].inplace_update(slot_index, 0)

    def __return_base_indirection__(self, RID):
        page_index, slot_index = self.page_directory[RID]
        indirection_index = self.base_range[INDIRECTION_COLUMN][page_index].read(slot_index)
        return indirection_index

    def __update__(self, columns):
        for column_index in range(self.num_columns + config.Offset):
            page_index = len(self.tail_range[column_index]) - 1
            slot_index = self.tail_range[column_index][page_index].write(columns[column_index])

            if slot_index == -1:
                self.__add_physical_tail_page__()
                self.tail_range[column_index][page_index + 1].write(columns[column_index]) #write to next page, therefore increment count on page_index
            self.page_directory[columns[RID_COLUMN]] = (page_index, slot_index) #on successful write, store to page directory

