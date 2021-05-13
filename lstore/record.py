from datetime import datetime
from lstore.config import *


class Record:
    def __init__(self, key, rid, base_rid, schema_encoding, column_values):
        self.primary_key = key
        # indirection, rid, schema_encoding, timestamp
        # https://stackoverflow.com/questions/14329794/get-size-of-integer-in-python
        date_time_object = datetime.now()
        date_time_int = int(date_time_object.strftime("%Y%m%d%H%M%S"))
        # 0 for indirection column
        self.meta_data = [0, rid, base_rid, date_time_int, schema_encoding]
        self.user_data = column_values
        self.all_columns = self.meta_data + self.user_data
        
    def get_rid(self):
        return self.meta_data[RID_COLUMN]
