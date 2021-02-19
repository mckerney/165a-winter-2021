from lstore.page import *
from lstore.helpers import *
import os 
"""
A data structure holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class IndividualIndex:
    def __init__(self,table,column_number):
        # pass
        # a map between column value and RID
        self.index = {}

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
            files = [entry for entry in os.listdir(table.table_path+'/'+page_range) if os.path.isfile(table.table_path+'/'+page_range+'/'+entry)]
            directories = [entry for entry in os.listdir(table.table_path+'/'+page_range) if not os.path.isfile(table.table_path+'/'+page_range+'/'+entry)]
            
            tail_pages = os.listdir(table.table_path+'/'+page_range+'/'+directories[0])


            for file in files:
                path_to_file = table.table_path+'/'+page_range+'/'+file
                print(path_to_file)

                column_page = Page( (META_COLUMN_COUNT+column_number))
                column_page.read_from_disk(path_to_file,META_COLUMN_COUNT+column_number)

                print(column_page.data)
                schema_page = Page( (SCHEMA_ENCODING_COLUMN))
                schema_page.read_from_disk(path_to_file,SCHEMA_ENCODING_COLUMN)

                rid_page = Page( (RID_COLUMN) )
                rid_page.read_from_disk(path_to_file, RID_COLUMN)

                indirection_page = Page( (INDIRECTION) )
                indirection_page.read_from_disk(path_to_file,INDIRECTION)

                for row in range(ENTRIES_PER_PAGE):
                    print("row",row)
                    schema_encode = schema_page.read(row)
                    
                    if get_bit(schema_encode,column_number) == 1:
                        indirection_rid = indirection_page.read(row)

                        # is this safe?
                        ind_dict = table.page_directory.get(indirection_rid)
                        page_range_index = ind_dict.get('page_range')
                        tail_page = ind_dict.get('tail_page')
                        tp_index = ind_dict.get('page_index')

                        # read tail page from information

                        physical_tail_page = Page((META_COLUMN_COUNT+column_number))
                        physical_tail_page.read(table.table_path+'/'+page_range+'/'+directories[0]+'/'+'tail_page_'+tail_page+'.bin')

                        val = physical_tail_page.read(row)
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
