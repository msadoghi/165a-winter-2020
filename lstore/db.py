from lstore.table import Table
from lstore.page import Page
import lstore.config
import math


#page_map: contains a list of base or tail ranges
class Bufferpool():
    def __init__(self, db):
        self.db = db
        self.frame_map = {} # page slot number to frame id
        self.page_map = {}  # frame id to page
        self.size = lstore.config.buffersize
        self.accesses = [0] * self.size

    def must_evict(self):
        return len(self.page_map) == self.size

    def fetch_range(self, name, page_slot):
        if page_slot in self.frame_map:
            self.accesses[self.frame_map[page_slot]] += 1
            return self.page_map[self.frame_map[page_slot]]
        else:
            new_range = self.get_range(name, page_slot)
            if self.must_evict():
                frame_num = self.evict(name)
                self.frame_map[page_slot] = frame_num
                self.page_map[frame_num] = new_range
            else:
                self.frame_map[page_slot] = len(self.page_map)
                self.page_map[self.frame_map[page_slot]] = new_range

        self.accesses[self.frame_map[page_slot]] += 1 #increase num accesses for this frame
        return self.page_map[self.frame_map[page_slot]]

    def get_range(self, name, page_index):
        curr_table = self.db.get_table(name)
        new_range = []
        for column_index in range(lstore.config.Offset + curr_table.num_columns):
            new_page = curr_table.disk.fetch_page(name, column_index, page_index) #fetch page from disk
            new_range.append(new_page)

        return new_range

    def add_range(self, name, page_slot):
        print("adding range")
        curr_table = self.db.get_table(name)
        new_range = []
        for column_index in range(lstore.config.Offset + curr_table.num_columns):
            new_page = Page()
            new_range.append(new_page)

        if self.must_evict():
            frame_num = self.evict(name)
            self.frame_map[page_slot] = frame_num
            self.page_map[frame_num]= new_range
            self.accesses[frame_num] += 1 #increase num accesses for this frame
            print("evicting frame from" + str(frame_num) + " new frame is " + str(page_slot))
        else:
            print(page_slot)
            self.frame_map[page_slot] = len(self.page_map)
            self.page_map[self.frame_map[page_slot]] = new_range
            self.accesses[self.frame_map[page_slot]] += 1 #increase num accesses for this frame

    def evict(self, name):
        print("in evict")
        count = math.inf
        evict_page_slot = None
        for fk in self.frame_map.keys():
            frame_num = self.frame_map[fk]
            num_accesses = self.accesses[frame_num]
            print("framenum is " + str(self.frame_map[fk]) + " page offset is " + str(fk) + " num_accesses is " + str(num_accesses))
            if num_accesses < count:
                count = num_accesses
                evict_page_slot = fk
                print("evict page slot: " + str(evict_page_slot))

        curr_table = self.db.get_table(name)
        for column_index in range(lstore.config.Offset + curr_table.num_columns):
            page_to_write = self.page_map[self.frame_map[evict_page_slot]][column_index]
            if page_to_write.dirty:
                curr_table.disk.write(name, column_index, evict_page_slot, page_to_write)

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
        for table in self.tables:
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
