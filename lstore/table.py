from lstore.page import *
from time import time
import lstore.config
from pathlib import Path
import os
import sys
import pickle

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


#only return if there is a page directory file specified, only happens after the db has been closed
def read_page_directory(name):
    file_name = os.getcwd() + lstore.config.DBName + "/" + name + "/page_directory.pkl"
    if os.path.exists(file_name):
        with open(file_name, 'rb') as file:
            return pickle.load(file)
    else:
        return {} #just return empty dict

def write_page_directory(name, page_directory):
    file_name = os.getcwd() + lstore.config.DBName + "/" + name + "/page_directory.pkl"
    with open(file_name, "wb") as file:
        pickle.dump(page_directory, file)

def read_counters(name):
    counters = []
    file_name = os.getcwd() + lstore.config.DBName + "/" + name + "/counters"
    if os.path.exists(file_name):
        with open(file_name, "rb") as file:
            for counter in range(6):
                counters.append(int.from_bytes(file.read(8), "big"))
        return counters

def write_counters(name, counters):
    file_name = os.getcwd() + lstore.config.DBName + "/" + name + "/counters"
    with open(file_name, "wb") as file:
        for counter in counters:
            file.write((counter).to_bytes(8, "big"))

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Disk():
    def __init__(self, name, num_columns):
        path_name = os.getcwd() + lstore.config.DBName + "/" + name
        if not os.path.exists(path_name):
            os.makedirs(path_name)

        for column_index in range(num_columns + lstore.config.Offset):
            filename = path_name + "/" + str(column_index) #name of the table / column number
            if not os.path.isfile(filename):
                file = open(filename, 'wb')
                empty_page = Page()
                file.write((0).to_bytes(4, "big"))
                file.write((empty_page.num_records).to_bytes(4, "big")) #this data is the number of records in the page

                file.write(empty_page.data)

    #fetch a page from disk at the column specified and directed to the offset
    def fetch_page(self, name, column_index, offset):
        path_name = os.getcwd() + lstore.config.DBName + "/" + name+ "/" + str(column_index)
        temp_page = Page()
        file = open(path_name, 'rb')

        file.seek(offset)
        tail_offset = int.from_bytes(file.read(4), "big")
        num_records = int.from_bytes(file.read(4), "big") #get first 8 bytes, convert to int to get num records
        page_data = bytearray(file.read(lstore.config.PageLength)) #binary file for the page data

        temp_page.num_records = num_records
        temp_page.data = page_data

        return temp_page

    def write(self, name, column_index, offset, page_to_write):
        path_name = os.getcwd() + lstore.config.DBName + "/" + name + "/" + str(column_index)
        file = open(path_name, 'r+b')

        file.seek(offset + 4) #skip the first parameter
        file.write(page_to_write.num_records.to_bytes(4, "big"))
        file.write(page_to_write.data)

    def update_offset(self, name, column_index, offset, offset_to_write):
        path_name = os.getcwd() + lstore.config.DBName + "/" + name + "/" + str(column_index)
        file = open(path_name, 'r+b')

        file.seek(offset)
        file.write(offset_to_write.to_bytes(4, "big"))

    #return the offset pointer for the specified disk 
    def get_offset(self, name, column_index, offset):
        path_name = os.getcwd() + lstore.config.DBName + "/" + name + "/" + str(column_index)
        file = open(path_name, 'rb')

        file.seek(offset)
        tail_offset = int.from_bytes(file.read(4), "big")
        return tail_offset

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
        self.sum = 0
        self.buffer = buffer_pool
        self.disk = None
        self.page_directory = {}

        self.base_RID = lstore.config.StartBaseRID
        self.tail_RID = lstore.config.StartTailRID
        self.base_offset_counter = 0
        self.tail_offset_counter = 0

    def __merge__(self):
        pass

    def __add_physical_base_range__(self):
        if self.base_offset_counter < self.tail_offset_counter:
            self.base_offset_counter = self.tail_offset_counter + lstore.config.FilePageLength 
        else:
            self.base_offset_counter += lstore.config.FilePageLength #increase offset after adding a range
        self.buffer.add_range(self.name, self.base_offset_counter)

    def __add_physical_tail_range__(self, previous_offset_counter):
        if self.tail_offset_counter < self.base_offset_counter:
            self.tail_offset_counter = self.base_offset_counter + lstore.config.FilePageLength 
        else:
            self.tail_offset_counter += lstore.config.FilePageLength #increase offset after adding a range
        self.buffer.add_range(self.name, self.tail_offset_counter)

        for column_index in range(lstore.config.Offset + self.num_columns): #update all the offsets
            self.disk.update_offset(self.name, column_index, previous_offset_counter, self.tail_offset_counter) #update offset value i


    def __read__(self, RID, query_columns):
        # What the fick tail index and tails slots?
        #print("RID here is " + str(RID))
        tail_index = tail_slot_index = -1
        page_index, slot_index = self.page_directory[RID]
        current_page = self.buffer.fetch_range(self.name, page_index)[INDIRECTION_COLUMN] #index into the physical location
        new_rid = current_page.read(slot_index)
        column_list = []
        key_val = -1
        if new_rid != 0:
            #print("new rid is: " + str(new_rid))
            tail_index, tail_slot_index = self.page_directory[new_rid] #store values from tail record
            current_tail_range = self.buffer.fetch_range(self.name, tail_index)
            for column_index in range(lstore.config.Offset, self.num_columns + lstore.config.Offset):
                if column_index == self.key + lstore.config.Offset:
                    #TODO TF is this shit, does it actually give the key val
                    key_val = query_columns[column_index - lstore.config.Offset]
                if query_columns[column_index - lstore.config.Offset] == 1:
                    current_tail_page = current_tail_range[column_index] #get tail page from 
                    column_val = current_tail_page.read(tail_slot_index)
                    column_list.append(column_val)

        else:
            current_base_range = self.buffer.fetch_range(self.name, page_index)
            for column_index in range(lstore.config.Offset, self.num_columns + lstore.config.Offset):
                if column_index == self.key + lstore.config.Offset:
                    key_val = query_columns[column_index - lstore.config.Offset] #subtract offset for the param columns

                if query_columns[column_index - lstore.config.Offset] == 1:
                    current_base_page = current_base_range[column_index]
                    column_val = current_base_page.read(slot_index)
                    #print("page index at " + str(page_index) + " slot index at " + str(slot_index) + " value is: " + str(column_val))
                    column_list.append(column_val)

        # check indir column record
        # update page and slot index based on if there is one or nah


        return Record(RID, key_val, column_list) #return proper record, or -1 on key_val not found

    def __insert__(self, columns):
        #returning any page in range will give proper size
        current_range = self.buffer.fetch_range(self.name, self.base_offset_counter)[0]
        if not current_range.has_capacity(): #if latest slot index is -1, need to add another range
            self.__add_physical_base_range__()

        page_index = self.base_offset_counter
        current_base_range = self.buffer.fetch_range(self.name, page_index)
        for column_index in range(self.num_columns + lstore.config.Offset):
            current_base_page = current_base_range[column_index]
            slot_index = current_base_page.write(columns[column_index])
        self.page_directory[columns[RID_COLUMN]] = (page_index, slot_index) #on successful write, store to page directory

    #in place update of the indirection entry.
    def __update_indirection__(self, old_RID, new_RID):
        page_index, slot_index = self.page_directory[old_RID]
        current_page = self.buffer.fetch_range(self.name, page_index)[INDIRECTION_COLUMN]
        current_page.inplace_update(slot_index, new_RID)

    def __update_schema_encoding__(self, RID):
        pass

    # Set base page entry RID to 0 to invalidate it
    def __delete__ (self, RID):
        page_index, slot_index = self.page_directory[RID]
        self.base_range[RID_COLUMN][page_index].inplace_update(slot_index, 0)

    def __return_base_indirection__(self, RID):
        page_index, slot_index = self.page_directory[RID]
        current_page = self.buffer.fetch_range(self.name, page_index)[INDIRECTION_COLUMN]
        indirection_index = current_page.read(slot_index)
        return indirection_index

    def __traverse_tail__(self, page_index):
        tail_offset = self.disk.get_offset(self.name, 0, page_index) #tail pointer at the specified base page in disk
        prev_tail = page_index
        while tail_offset != 0: 
            prev_tail = tail_offset
            tail_offset = self.disk.get_offset(self.name, 0, prev_tail)

        tail_offset = prev_tail
        return tail_offset

    def __update__(self, columns, base_rid):
        #print("self.base_offset_counter is " + str(self.base_offset_counter) + " self.tail_offset_counter is " + str(self.tail_offset_counter))
        base_offset, _ = self.page_directory[base_rid]

        current_tail = None
        previous_offset = self.__traverse_tail__(base_offset)
        page_offset = previous_offset
        if previous_offset == base_offset: #if there is no tail page for the base page
            #print("adding range for base page")
            self.__add_physical_tail_range__(previous_offset)
            page_offset = self.tail_offset_counter

        current_tail = self.buffer.fetch_range(self.name, page_offset)[0]
        if not current_tail.has_capacity(): #if the latest tail page is full
            #print("adding new tail page to existing range")
            self.__add_physical_tail_range__(previous_offset)
            page_offset = self.tail_offset_counter
            #add the new range and update the tail offsets accordingly 

        current_tail_range = self.buffer.fetch_range(self.name, page_offset)
        #print("tail range fetched successfully")
        for column_index in range(self.num_columns + lstore.config.Offset):
            #print(columns[column_index], end = " ")
            current_tail_page = current_tail_range[column_index]
            slot_index = current_tail_page.write(columns[column_index])
        self.page_directory[columns[RID_COLUMN]] = (page_offset, slot_index) #on successful write, store to page directory