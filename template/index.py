from template.table import Table

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        self.table = table
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

    def create_index(self, table, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, table, column_number):
        pass
