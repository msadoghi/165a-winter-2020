from table import Table, Record
from index import Index
from time import process_time
import struct
import config
from datetime import datetime

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table
        self.index = Index(self.table)
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        pass

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        timestamp = process_time()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        indirection_index = 0
        key_index = self.table.key
        rid = self.table.base_RID
        columns = [indirection_index, rid, timestamp, schema_encoding] + list(columns)
        # print(columns)
        self.table.__insert__(columns) #table insert
        self.index.create_index(rid, columns[key_index + config.Offset]) #account for offset by adding 4
        self.table.base_RID += 1
        pass

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        rid = self.index.locate(key)
        result = self.table.__read__(rid, query_columns)
        return (-1 if rid == 1 else result)
        pass

    """
    # Update a record with specified key and columns
    """

    #TODO change schema encoding, RID of base page here??
    def update(self, key, *columns):
        schema_encoding = '0' * self.table.num_columns
        timestamp = process_time()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        indirection_index = 0
        rid = self.table.tail_RID
        columns = [indirection_index, rid, timestamp, schema_encoding] + list(columns)
        self.table.__update__(key, columns)

        old_rid = self.index.locate(key) # base record, do not update index only insert
        self.table.__update_indirection__(rid, old_rid) #tail record gets base record's RID
        self.table.__update_indirection__(old_rid, rid) #base record gets latest update RID
        self.table.tail_RID -= 1
        pass

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
