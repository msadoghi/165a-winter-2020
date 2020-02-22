from lstore.table import Table
import lstore.config
import math


class Bufferpool():
    def __init__(self, db):
        self.db = db
        self.frame_map = {} # page slot number to frame id
        self.page_map = {}  # frame id to page
        self.size = lstore.config.buffersize

    def must_evict(self):
        return len(self.page_map) == self.size

    def fetch_page(self, name, column_index, page_slot):
        if page_slot in self.frame_map:
            return self.page_map[self.frame_map[page_slot]]
        else:
            curr_table = self.db.get_table(name)
            new_page = curr_table.disk.fetch_page(name, column_index, page_slot) #fetch page from disk
            if self.must_evict():
                frame_num = self.evict(name, column_index)
                self.frame_map[page_slot] = frame_num
                self.page_map[frame_num]= new_page
            else:
                self.frame_map[page_slot] = len(self.page_map)
                self.page_map[self.frame_map[page_slot]] = new_page

        return self.page_map[self.frame_map[page_slot]]

    def add_page(self, name, column_index):
        new_page = Page()
        if self.must_evict():
            self.frame_num = self.evict()
            self.frame_map[page_slot] = frame_num
            self.page_map[frame_num]= new_page
        else:
            self.frame_map[page_slot] = len(self.page_map)
            self.page_map[frame_map[page_slot]] = new_page

    def evict(self, name, column_index):
        count = math.inf
        evict_page_slot = None
        print("check")
        for fk in self.frame_map.keys():
            print(fk)
            print(self.frame_map[fk])
            num_accesses = self.page_map[self.frame_map[fk]].access_count
            if count > num_accesses:
                count = num_accesses
                evict_page_slot = fk

        curr_table = self.db.get_table(name)
        curr_table.disk.write(name, column_index, evict_page_slot, self.page_map[self.frame_map[evict_page_slot]])
        return self.frame_map[evict_page_slot]

class Database():

    def __init__(self):
        self.tables = []
        self.buffer_pool = Bufferpool(self)
        pass

    def open(self, db_name):
        lstore.config.DBName = db_name
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.buffer_pool)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
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
        pass
