from lstore.page import *
from lstore.helpers import *
from os import os 
"""
A data structure holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class IndividualIndex:
    def __init__(self,table,column_number):
        # a map between column value and RID
        # self.index = {}
        # for page_range in table.page_ranges:
        #     for base_page in page_range.base_pages:
        #         for i in range(ENTRIES_PER_PAGE):
        #             schema_encode = base_page.columns_list[SCHEMA_ENCODING_COLUMN].read(i)
        #             if get_bit(schema_encode,column_number - META_COLUMN_COUNT):
        #                 indirection_rid = base_page.columns_list[INDIRECTION].read(i)
        #                 ind_dict = table.page_directory.get(indirection_rid)
        #                 page_range_index = ind_dict.get('page_range')
        #                 tail_page = ind_dict.get('tail_page')
        #                 tp_index = ind_dict.get('page_index')
        #                 val = table.page_ranges[page_range_index].tail_pages[tail_page].column_list[column_number].read(i)
        #                 self.index[val] = (self.index[val] or []).append(indirection_rid)
        #             else:
        #                 val = base_page.columns_list[column_number].read(i)
        #                 rid = base_page.columns_list[RID_COLUMN].read(i)
        #                 self.index[val] = (self.index[val] or []).append(rid)
        # instead of looping through page ranges, loop through page range directories
        # each file inside page range directory is a base page


        for page_range in os.listdir(table.table_path):
            files = [entry for entry in os.listdir(table.table_path+'/'+page_range) if os.path.isfile(entry)]
            directories = [entry for entry in os.listdir(table.table_path+'/'+page_range) if not os.path.isfile(entry)]
            tail_pages = 0
            if len(directories) > 0:
                tail_pages = os.listdir(table.table_path+'/'+page_range+'/'+directories[0])


            for file in files:
                path_to_file = table.table_path+'/'+page_range+'/'+file
                column_file = open(path_to_file,"rb")
                column_beginning = (META_COLUMN_COUNT+column_number)*PAGE_SIZE
                column_file.seek(column_beginning)
                column_data = bytearray(column_file.read(PAGE_SIZE))

                schema_file = open(path_to_file,"rb")
                schema_beginning = (SCHEMA_ENCODING_COLUMN)*PAGE_SIZE
                schema_file.seek(schema_beginning)
                schema_data = bytearray(schema_file.read(PAGE_SIZE))

                rid_file = open(path_to_file,"rb")
                rid_beginning = (RID_COLUMN)*PAGE_SIZE
                rid_file.seek(rid_beginning)
                rid_data = bytearray(rid_file.read(PAGE_SIZE))

                indirection_file = open(path_to_file,"rb")
                indirection_beginning = (INDIRECTION)*PAGE_SIZE
                indirection_file.seek(indirection_beginning)
                indirection_data = bytearray(indirection_file.read(PAGE_SIZE))


                for row in range(ENTRIES_PER_PAGE):
                    schema_sp = schema_beginning + (row * PAGE_RECORD_SIZE)
                    schema_encode = schema_data[schema_sp:(schema_sp+PAGE_RECORD_SIZE)]
                    
                    if get_bit(schema_encode,column_number - META_COLUMN_COUNT):
                        indirection_sp = indirection_beginning + (row * PAGE_RECORD_SIZE)
                        indirection_rid = indirection_data[indirection_sp:(indirection_sp+PAGE_RECORD_SIZE)]

                        # is this safe?
                        ind_dict = table.page_directory.get(indirection_rid)
                        page_range_index = ind_dict.get('page_range')
                        tail_page = ind_dict.get('tail_page')
                        tp_index = ind_dict.get('page_index')

                        # read tail page from 
                        val = table.page_ranges[page_range_index].tail_pages[tail_page].column_list[column_number].read(i)
                        self.index[val] = (self.index[val] or []).append(indirection_rid)
                    else:
                        val = base_page.columns_list[column_number].read(i)
                        rid = base_page.columns_list[RID_COLUMN].read(i)
                        self.index[val] = (self.index[val] or []).append(rid)


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
