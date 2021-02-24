from lstore.page import *
from lstore.helpers import *
import os 
import math
"""
A data structure holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class IndividualIndex:
    def __init__(self,table,column_number):
        # pass
        # a map between column value and RID
        self.index = {}

        table.bufferpool.commit_all_frames()

        last_rid = table.num_base_records - 1

        last_page_range = math.floor(last_rid/ ENTRIES_PER_PAGE_RANGE)
        index = last_rid% ENTRIES_PER_PAGE_RANGE
        last_base_page = math.floor(index / ENTRIES_PER_PAGE)
        last_physical_page_index = index % ENTRIES_PER_PAGE

        print(f'last page range:{last_page_range}')
        print(f'last base page:{last_base_page}')


        for page_range_index in range(0,last_page_range+1):
            page_range_path = f'{table.table_path}/page_range_{page_range_index}'

            last_base_page_index = BASE_PAGE_COUNT
            if page_range_index == last_page_range:
                last_base_page_index = last_base_page  
            for base_page_index in range(0,last_base_page_index+1):
                path_to_file = f'{page_range_path}/base_page_{base_page_index}.bin'
                print(path_to_file)

                column_page = Page( (META_COLUMN_COUNT+column_number))
                column_page.read_from_disk(path_to_file,META_COLUMN_COUNT+column_number)

                schema_page = Page( (SCHEMA_ENCODING_COLUMN))
                schema_page.read_from_disk(path_to_file,SCHEMA_ENCODING_COLUMN)

                rid_page = Page( (RID_COLUMN) )
                rid_page.read_from_disk(path_to_file, RID_COLUMN)

                indirection_page = Page( (INDIRECTION) )
                indirection_page.read_from_disk(path_to_file,INDIRECTION)

                last_row_index = ENTRIES_PER_PAGE
                if(base_page_index == last_base_page_index):
                    last_row_index = last_physical_page_index+1

                for row in range(last_row_index):
                    schema_encode = schema_page.read(row)
                    schema_encode_bit = get_bit(schema_encode,column_number)

                    rid = rid_page.read(row)
                    base_ind_dict = table.page_directory.get(rid)
                    row_deleted = base_ind_dict.get('deleted')
                    if(row_deleted):
                        continue

                    if schema_encode_bit == 1:
                        indirection_rid = indirection_page.read(row)
                        tail_ind_dict = table.page_directory.get(indirection_rid)

                        tail_page = tail_ind_dict.get('tail_page')

                        # read tail page from information

                        physical_tail_page = Page((META_COLUMN_COUNT+column_number))
                        tail_path = f'{page_range_path}/tail_pages/tail_page_{tail_page}.bin'
                        physical_tail_page.read_from_disk(tail_path,META_COLUMN_COUNT+column_number)

                        val = physical_tail_page.read(row)
                        
                        print("TAIL VAL",val)
                        self.index[val] = (self.index.get(val) or []) + [rid]
                    else:
                        val = column_page.read(row)
                        print("BASE VAL",val)
                        self.index[val] = (self.index.get(val) or [])+[rid]

        print(self.index)


    # returns set of rids containing given value
    def get(self,value):
        return self.index.get(value)

    def insert(self,value,new_rid):
        self.index[value] = (self.index.get(value) or []) + [new_rid]
    
    def update(self,new_value,old_value, base_rid):
        # remove base rid from the old value's entry
        self.index[old_value].remove(base_rid)
        # add base rid to new value's entry
        self.index[new_value] = (self.index.get(new_value) or []) + [base_rid]

    def delete(self,value,deleted_rid):
        self.index[value].remove(deleted_rid)

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table
        pass
    
    def create_default_primary_index(self):
        self.create_index(0)

    """
    # returns the IndividualIndex for given column
    """

    def get_index_for_column(self,column):
        return self.indices[column]

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.indices[column_number] = IndividualIndex(self.table,column_number)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if(column_number == 0):
            print('you cannot remove the primary key index')
            return
        self.indices[column_number] = None
