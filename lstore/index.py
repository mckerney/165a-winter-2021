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
                    print("row",row)
                    schema_encode = schema_page.read(row)
                    print("SCHEMA_ENCODING!",schema_encode)
                    schema_encode_bit = get_bit(schema_encode,column_number)
                    print(f'SCHEMA ENCODE BIT {schema_encode_bit}')
                    if schema_encode_bit == 1:
                        indirection_rid = indirection_page.read(row)

                        # is this safe?
                        ind_dict = table.page_directory.get(indirection_rid)
                        page_range_index = ind_dict.get('page_range')
                        tail_page = ind_dict.get('tail_page')

                        # read tail page from information

                        physical_tail_page = Page((META_COLUMN_COUNT+column_number))
                        tail_path = f'{page_range_path}/tail_pages/tail_page_{tail_page}.bin'
                        physical_tail_page.read_from_disk(tail_path,META_COLUMN_COUNT+column_number)

                        val = physical_tail_page.read(row)
                        rid = rid_page.read(row)
                        print("TAIL VAL",val)
                        try:
                            self.index[val] = self.index[val]+[rid]
                        except KeyError:
                            self.index[val] = [rid] 
                    else:
                        val = column_page.read(row)
                        print("BASE VAL",val)
                        rid = rid_page.read(row)
                        try:
                            self.index[val] = self.index[val]+[rid]
                            
                        except KeyError:
                            self.index[val] = [rid] 

        print(self.index)
        pass


    # returns set of rids containing given value
    def get(self,value):
        return self.index[value]

    def insert(self,value,new_rid):
        self.index[value] = self.index[value].append(new_rid)
    
    def update(self,value,new_rid,old_rid):
        # replace old rid with new rid
        # self.index[value] = [x if x!=old_rid else new_rid for x in self.index[value]]
        pass

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        return self.indices[column].get(value)

    # """
    # # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    # """

    # def locate_range(self, begin, end, column):
    #     pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.indices[column_number] = IndividualIndex(self.table,column_number)
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
        pass
