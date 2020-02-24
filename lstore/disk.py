from lstore.page import *
from time import time
import lstore.config
from pathlib import Path
import os
import sys
import pickle

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