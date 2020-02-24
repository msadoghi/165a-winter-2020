from lstore.table import *
from lstore.disk import *
from lstore.buffer import *
from lstore.page import Page
import lstore.config
import lstore.table
import math
import os

class Database():
    def __init__(self):
        self.tables = []
        self.buffer_pool = Bufferpool(self)
        pass

    def open(self, db_name):
        lstore.config.DBName = db_name
        path_name = os.getcwd() + db_name
        if not os.path.exists(path_name): #create DB directory
            os.makedirs(path_name)

        for root, subdirs, files in os.walk(path_name): #initialize table by loading from disk
            for name in subdirs:
                table_name = name
                table = Table(table_name, 0, 0, self.buffer_pool)
                table.page_directory = read_page_directory(table.name)
                table.key, table.num_columns, table.base_RID, table.tail_RID, table.base_offset_counter, table.tail_offset_counter = lstore.table.read_counters(table.name)
                table.disk = Disk(table.name, table.num_columns)
                self.tables.append(table)

    def close(self):
        for table in self.tables:
            write_page_directory(table.name, table.page_directory) #write page_directory to file
            write_counters(table.name, [table.key, table.num_columns, table.base_RID, table.tail_RID, table.base_offset_counter, table.tail_offset_counter])

            #write pages to disk only if dirty
            for page_index in self.buffer_pool.frame_map.keys():
                for column_index in range(table.num_columns + lstore.config.Offset):
                    frame_num = self.buffer_pool.frame_map[page_index]
                    page_to_write = self.buffer_pool.page_map[frame_num][column_index]
                    if page_to_write.dirty:
                        table.disk.write(table.name, column_index, page_index, page_to_write)


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.buffer_pool)
        table.disk = Disk(table.name, table.num_columns)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name): #TODO: remove disk space??
        for table in self.tables:
            if table.name == name:
                del table

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table

        return -1
