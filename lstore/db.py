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
        return len(page_map) == self.size

    def fetch_page(self, page_slot):
        if page_slot in frame_map:
            return page_map[frame_map[page_slot]]
        else:
            new_page = self.db.get_page(page_slot)
            if self.must_evict():
                frame_num = self.evict()
                frame_map[page_slot] = frame_num
                frame_map[frame_num]= new_page
            else:
                frame_map[page_slot] = len(page_map)
                page_map[frame_map[page_slot]] = new_page

    def evict(self):
        count = math.inf
        evict_page_slot = None
        for fk in frame_map.keys()
             count = (page_map[frame_map[fk]].access_count if count > page_map[frame_map[fk]].access_count)
             evict_page_slot = (fk if count > page_map[frame_map[fk]].access_count)
        self.db.write(evict_page_slot, page_map[frame_map[evict_page_slot]])
        return frame_map[evict_page_slot]

class Database():

    def __init__(self):
        self.tables = []
        self.buffer_pool = Bufferpool(self)
        pass

    def open(self):
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
        table = Table(name, num_columns, key)
        table.buffer = self.buffer_pool
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
        pass
