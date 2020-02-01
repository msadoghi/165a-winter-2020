from table import Table, Record
from index import Index
from time import process_time
import struct
import config
from datetime import datetime

def compare_cols(old_columns, new_columns): #if any new_columns are None type, give it the old_columns values
        for column_index in range(len(new_columns)):
            new_columns[column_index] = (old_columns[column_index] if new_columns[column_index] is None else new_columns[column_index])   
        return new_columns

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
        return (-1 if rid == -1 else result)
        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        schema_encoding = '0' * self.table.num_columns
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        indirection_index = 0
        rid = self.table.tail_RID
        old_columns = self.select(key, [1] * self.table.num_columns).columns #get every column and compare to the new one: cumulative update
        new_columns = list(columns)
        columns = [indirection_index, rid, timestamp, schema_encoding] + compare_cols(old_columns, new_columns)
        self.table.__update__(columns) #add record to tail pages 

        old_rid = self.index.locate(key)
        old_indirection =  self.table.__return_base_indirection__(old_rid)# base record, do not update index only insert

        self.table.__update_indirection__(rid, old_indirection) #tail record gets base record's indirection index
        self.table.__update_indirection__(old_rid, rid) #base record's indirection column gets latest update RID

        #TODO: update schema encoding
        self.table.tail_RID -= 1
        pass

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
