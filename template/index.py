from table import Table
from collections import defaultdict

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        self.table = table
        self.index_list = []
        for column in self.num_columns:
            column_dict = defaultdict(lambda: [])
            self.index_list.append(column_dict)
        pass

    """
    # returns the location of all records with the given value
    """

    def locate(self, value):
        reckerds = []
        for key in self.table.page_directory:
            for page in self.table.page_directory[key]:
                i = 0
                while i < len(page):
                    if value.to_byte(8, "big") == page[i:i+8]:
                        reckerds.append((key, i))
        return reckerds


    """
    # optional: Create index on specific column
    """
    #TODO: CHANGE THIS????
    def create_index(self, column_number):
        col_index = self.table.page_range[column_number] #page at the specified page range
        dict_index = self.index_list[col_index]
        for page_index in range(len(col_index)): #go through every page
            dict_index[col_index].append(self.table.page_range[table.RID_COLUMN]) 
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
