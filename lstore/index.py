from lstore.table import Table
from collections import defaultdict

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        self.table = table
        self.index_dict = {} # key is primary key; value is rid
        pass

    """
    # returns the location of all records with the given value
    """

    def locate(self, value):
        if value in self.index_dict:
            return self.index_dict[value]
        return -1


    """
    # optional: Create index on specific column
    """
    def create_index(self, RID, primary_key):
        if primary_key not in self.index_dict:
            self.index_dict[primary_key] = RID
            return 0
        return -1 # cant have dup primary keys

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
