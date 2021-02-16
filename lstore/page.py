from lstore.config import *
from datetime import datetime
import struct
from math import ceil
import time

class Page:

    def __init__(self, column_num):
        self.num_records = 0
        self.column_num = column_num
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        current_empty_space = ENTRIES_PER_PAGE - self.num_records
        if current_empty_space == 0:
            return False
        elif current_empty_space > 0:
            return True
        else:
            raise ValueError('ERROR: current_empty_space must be greater or equal to zero.')

    def write(self, value, row) -> bool:
        if row > ENTRIES_PER_PAGE: # row number is larger than 511
            return False
        else:
            starting_point = row * PAGE_RECORD_SIZE
            self.data[starting_point:(starting_point + PAGE_RECORD_SIZE)] = value.to_bytes(8, 'big')
            self.num_records += 1
            return True

    def read(self, row) -> int:
        starting_point = row * PAGE_RECORD_SIZE
        ret_value_in_bytes = self.data[starting_point:(starting_point + PAGE_RECORD_SIZE)]
        int_val = int.from_bytes(bytes=ret_value_in_bytes, byteorder="big")
        return int_val

    def read_from_disk(self, path_to_page: str, row: int) -> bool:
        bin_file = open(path_to_page, "rb")
        bin_file.seek(row*PAGE_SIZE)
        self.data = bytearray(bin_file.read(PAGE_SIZE))
        bin_file.close()

    def write_to_disk(self, path_to_page: str, row: int) -> bool:
        bin_file = open(path_to_page, "ab")
        bin_file.seek(row*PAGE_SIZE)
        bin_file.write(self.data)
        bin_file.close()



