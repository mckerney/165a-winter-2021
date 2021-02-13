from lstore.config import * 
from lstore.page import *
from lstore.table import *

class Bufferpool:

    def __init__(self):
        self.frames = [Frame() for i in range(BUFFERPOOL_FRAME_COUNT)]
        self.frame_directory = {
            # "table_name" : {
            #           "RIDS" : { rid: frame_index, ... },
            #           "BPS"  : { base_page_index: frame_index, ... },
            #           "TPS"  : { tail_page_index: frame_index, ... }
            # }
        }
        self.frame_count = 0
        # TODO need to intiialize frame_directory after Table is created in the DB

    def _add_frame_to_directory(self,page_range,base_page):
        new_frame_key = (page_range,base_page)
        self.frame_directory[new_frame_key] = self.frame_count
        self.frame_count += 1


    def at_capacity(self):
        if self.frame_count < BUFFERPOOL_FRAME_COUNT:
            return False
        else:
            return True    


    def is_rid_in_pool(self, table_name: str, rid: int) -> bool:
        '''
        Checks if a RID for a given table_name exists in the BufferPool and returns
        True if it does, False otherwise
        '''
        
        table_rids = self.frame_directory.get(table_name).get('RIDS')

        if rid in table_rids.keys():
            return True

        return False
    

    def get_rid_frame_index(self, table_name: str, rid: int) -> int:
        '''
        Return bufferpool frame index for a table's rid
        '''

        return self.frame_directory.get(table_name).get('RIDS').get(rid)


    def is_base_page_in_pool(self, table_name: str, bp_index: int) -> bool:
        '''
        Checks if a BasePage for a given table_name exists in the BufferPool and returns
        True if it does, False otherwise
        '''

        table_bps = self.frame_directory.get(table_name).get('BPS')

        if bp_index in table_bps.keys():
            return True
        
        return False
    

    def get_base_page_frame_index(self, table_name: str, bp_index: int) -> int:
        '''
        Return bufferpool frame index for a table's BasePage
        '''

        return self.frame_directory.get(table_name).get('BPS').get(bp_index)


    def is_tail_page_in_pool(self, table_name: str, tp_index: int) -> bool:
        '''
        Checks if a TailPage for a given table_name exists in the BufferPool and returns
        True if it does, False otherwise
        '''

        table_tps = self.frame_directory.get(table_name).get('TPS')

        if tp_index in table_tps.keys():
            return True
        
        return False
    

    def get_tail_page_frame_index(self, table_name: str, tp_index: int) -> int:
        '''
        Return bufferpool frame index for a table's TailPage
        '''

        return self.frame_directory.get(table_name).get('TPS').get(tp_index)


    def evict_page(self):
        pass

    def load_page(self, base_page, page_range, column_num):
        pass

    def commit_page(self):
        pass


class Frame:

    def __init__(self, table_name):

        self.page = None # Initialize at none since different tables have different column counts
        self.dirty_bit = False
        self.pin = False
        self.time_in_bufferpool = 0
        self.access_count = 0 #number of times page has been accessed
        self.table_name = table_name

    
    def set_dirty_bit(self):
        self.dirty_bit = True
        return True

    def pin_frame(self):
        self.pin = True
        return True

    def unpin_frame(self):
        self.pin = False
        return True
    

