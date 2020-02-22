from lstore.page import *
from time import time
import lstore.config
from pathlib import Path
import os

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Disk():
    def __init__(self, name, num_columns):
        path_name = os.getcwd() + "/" + name
        if not os.path.exists(path_name):
            os.makedirs(path_name)

        for column_index in range(num_columns + lstore.config.Offset):
            filename = path_name + "/" + str(column_index) #name of the table / column number
            if not os.path.isfile(filename):
                file = open(filename, 'wb+')
                print("empty")
                empty_page_data = bytearray(lstore.config.PageLength)
                file.write((0).to_bytes(8, "big")) #this data is the number of records in the page
                file.write(empty_page_data)

    def fetch_page(self, name, column_index, offset):
        path_name = os.getcwd() + "/" + name + "/" + str(column_index)
        temp_page = Page()
        print(temp_page.data)
        file = open(path_name, 'rb')

        file.seek(offset)
        num_records = int.from_bytes(file.read(8), "big") #get first 8 bytes, convert to int to get num records
        page_data = bytearray(file.read(lstore.config.PageLength)) #binary file for the page data

        temp_page.num_records = num_records
        temp_page.data = page_data

        return temp_page

    def write(self, name, column_index, offset, page_to_write):
        path_name = os.getcwd() + "/" + name + "/" + str(column_index)
        file = open(path_name, 'w+b')

        file.seek(offset)
        file.write(page_to_write.num_records.to_bytes(8, "big"))
        file.write(page_to_write.data)

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    :param base_RID/tail_RID    #start indexes for records for tail and base ranges
    :param base_range/tail_range #in memory representation of page storage
    """
    def __init__(self, name, num_columns, key, buffer_pool):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.sum = 0
        self.buffer = buffer_pool
        self.disk = Disk(self.name, self.num_columns)

        self.base_RID = lstore.config.StartBaseRID
        self.tail_RID = lstore.config.StartTailRID
        self.base_range = []
        self.tail_range = []

        self.offset_counter = 0

        #populate page range with base pages, which is a list of physical pages
        for index in range(self.num_columns + lstore.config.Offset):
            base_page = []
            base_page.append(Page())
            self.base_range.append(base_page)

        for index in range(self.num_columns + lstore.config.Offset):
            tail_page = []
            tail_page.append([Page()])
            self.tail_range.append(tail_page)

    def __merge__(self):
        pass

    def __add_physical_base_page__(self):
        for column_index in range(self.num_columns + lstore.config.Offset):
            self.buffer.add_page(self, self.name, column_index)

        self.offset_counter += lstore.config.FilePageLength

    def __add_physical_tail_page__(self, page_range_index):
        for page_index in range(self.num_columns + lstore.config.Offset):
            self.tail_range[page_index][page_range_index].append(Page()) #add a page at the current column index

    def __read__(self, RID, query_columns):
        # What the fick tail index and tails slots?
        tail_index = tail_slot_index = -1
        page_index, slot_index = self.page_directory[RID]
        current_page = self.buffer.fetch_page(self.name, INDIRECTION_COLUMN, page_index) #index into the physical location
        new_rid = current_page.read(slot_index)

        column_list = []
        key_val = -1

        if new_rid != 0:
            tail_index, tail_slot_index = self.page_directory[new_rid] #store values from tail record

            for column_index in range(lstore.config.Offset, self.num_columns + lstore.config.Offset):
                if column_index == self.key + lstore.config.Offset:
                    #TODO TF is this shit, does it actually give the key val
                    key_val = query_columns[column_index - lstore.config.Offset]
                if query_columns[column_index - lstore.config.Offset] == 1:
                    current_tail_page = self.buffer.fetch_page(self.name, column_index, tail_index) #get tail page from 
                    column_val = current_tail_page.read(tail_slot_index)
                    column_list.append(column_val)

        else:
            for column_index in range(lstore.config.Offset, self.num_columns + lstore.config.Offset):
                if column_index == self.key + lstore.config.Offset:
                    key_val = query_columns[column_index - lstore.config.Offset] #subtract offset for the param columns

                if query_columns[column_index - lstore.config.Offset] == 1:
                    current_base_page = self.buffer.fetch_page(self.name, column_index, page_index)
                    column_val = current_base_page.read(slot_index)
                    column_list.append(column_val)
        # check indir column record
        # update page and slot index based on if there is one or nah


        return Record(RID, key_val, column_list) #return proper record, or -1 on key_val not found

    def __insert__(self, columns):

        for column_index in range(self.num_columns + lstore.config.Offset):
            page_index = self.offset_counter
            current_base_page = self.buffer.fetch_page(self.name, column_index, page_index)
            #page_index = len((self.base_range[column_index])) - 1 #start at the latest page since everything else is full
            slot_index = current_base_page.num_records

            if slot_index == 512: #if latest slot index is -1, need to add another page
                self.__add_physical_base_page__(column_index)
                self.offset_counter += lstore.config.FilePageLength #increase offset if in next page
                page_index = self.offset_counter
                current_base_page = self.buffer.fetch_page(self.name, column_index, page_index)
                slot_index = current_base_page.num_records
                current_base_page.write(columns[column_index])
                #self.base_range[column_index][page_index + 1].write(columns[column_index])

            self.page_directory[columns[RID_COLUMN]] = (page_index, slot_index) #on successful write, store to page directory

    #in place update of the indirection entry. The third flag is a boolean set based on which page range written to
    def __update_indirection_tail__(self, new_RID, old_RID, base_RID):
        base_page_index, _ = self.page_directory[base_RID]
        tail_page_index, slot_index = self.page_directory[new_RID]
        self.tail_range[INDIRECTION_COLUMN][base_page_index][tail_page_index].inplace_update(slot_index, old_RID)

    def __update_indirection_base__(self, old_RID, new_RID):
        page_index, slot_index = self.page_directory[old_RID]
        self.base_range[INDIRECTION_COLUMN][page_index].inplace_update(slot_index, new_RID)

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

    def __update__(self, columns, base_rid):
        page_range_index, _ = self.page_directory[base_rid]
        for column_index in range(self.num_columns + lstore.config.Offset):
            page_index = len(self.tail_range[column_index][page_range_index]) - 1
            slot_index = self.tail_range[column_index][page_range_index][page_index].write(columns[column_index])

            if slot_index == -1:
                self.__add_physical_tail_page__(page_range_index)
                self.tail_range[column_index][page_range_index][page_index + 1].write(columns[column_index]) #write to next page, therefore increment count on page_index
            self.page_directory[columns[RID_COLUMN]] = (page_index, slot_index) #on successful write, store to page directory
