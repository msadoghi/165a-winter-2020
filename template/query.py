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
    #TODO create page directory, page for each column
    def __init__(self, table):
        self.table = table
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

    #create a record
    # save each column of the record to a page
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns

        #START: our code
        #TODO we need to figure out how to convert timestamp to bytes
        timestamp = process_time()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        indirection_index = 0
        rid = config.StartBaseRID
        columns = [indirection_index, rid, timestamp, schema_encoding] + list(columns)
        print(columns)
        self.table.__insert__(columns)
        config.StartBaseRID += 1
        pass

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        pass

    """
    # Update a record with specified key and columns
    """

    #TODO implement tail page logic
    def update(self, key, *columns):
        schema_encoding = '0' * self.table.num_columns
        timestamp = process_time()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        indirection_index = 0
        rid = config.StartTailRID
        columns = [indirection_index, rid, timestamp, schema_encoding] + list(columns)
        self.table.__update__(key, columns)
        pass

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
