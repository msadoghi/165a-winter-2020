from lstore.table import Table
from collections import defaultdict
from btree import BTreeNode, BTree

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        self.table = table
        self.index_dict = {}
        # key is primary key; value is rid
        # Want to go through num_columns and initialize Btrees

        # Create dictionaries for each of the columns
        for i in range(self.table.num_columns):
            index_dict[i] = {}
        pass

    """
    # returns the location of all records with the given value
    """
    # Add another parameter, column, so we can specify the column we want to find
    def locate(self, value, column):
        # BTree = index_dict[column]
        # BTree.search()

        # Locate certain values once we pass the column and the value
        if value in self.index_dict[column]:
            return self.index_dict[[column], [value]]
        return -1

    """
    # optional: Create index on specific column
    """
    #If key not in our dict, we addend the RID to the column, else we create new index
    def create_index(self, RID, key column):
        if key not in self.index_dict[column]:
            self.index_dict[column][key].append(RID)
            return 0
        else:
            self.index_dict[column][key] = [RID]

    """
    # optional: Drop index of specific column
    """
    # Delete given column_number
    def drop_index(self, column_number):
        del self.index[column_number]
        pass

    # Function to add RIDS from a certain range
    def range(self, start, end, column):
        RIDS = []
        for i in range(start, end + 1):
            RID += self.locate(i, column)
