from table import Table, Record
from index import Index
from time import process_time
import struct
import config
from datetime import datetime

tailRID = config.StartTailRID
baseRID = config.StartBaseRID

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

        #START: our code
        #TODO we need to figure out how to convert timestamp to bytes
        timestamp = process_time()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        indirection_index = 0
        rid = config.StartBaseRID
        columns = [indirection_index, rid, timestamp, schema_encoding] + list(columns)
        print(columns)
        self.table.__insert__(columns)
        baseRID += 1
        pass

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
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
        rid = config.StartTailRID
        columns = [indirection_index, rid, timestamp, schema_encoding] + list(columns)
        self.table.__update__(key, columns)
        tailRID -= 1
        pass

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
