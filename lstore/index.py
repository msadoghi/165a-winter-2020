from lstore.table import Table
import lstore.config
from collections import defaultdict

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        self.table = table
        self.index_dict = {} # key is primary key; value is rid
        for i in range(self.table.num_columns):
            self.index_dict[i] = {}
        pass

    """
    # returns the location of all records with the given value
    """

    def locate(self, value, column):
        if column in self.index_dict:
            if value not in self.index_dict[column]:
                return []
            else:
                return self.index_dict[column][value]
        return -1


    """
    # optional: Create index on specific column
    """
    def create_index(self, RID, cols):
        for i in range(len(cols)):

            # Check for duplicate primary keys
            if i ==  self.table.key + lstore.config.Offset:
                if cols[i] in self.index_dict[i]:
                    return -1

            if cols[i] not in self.index_dict[i]:
                self.index_dict[i][cols[i]] = [RID]
            else:

                self.index_dict[i][cols[i]].append(RID)
        return 0


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
