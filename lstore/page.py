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
        if row > ENTRIES_PER_PAGE:  # row number is larger than 511
            return False

        starting_point = row * PAGE_RECORD_SIZE
        self.data[starting_point:(starting_point + PAGE_RECORD_SIZE)] = value.to_bytes(8, 'big')
        self.num_records += 1
        return True

    def read(self, row) -> int:
        starting_point = row * PAGE_RECORD_SIZE
        ret_value_in_bytes = self.data[starting_point:(starting_point + PAGE_RECORD_SIZE)]
        int_val = int.from_bytes(bytes=ret_value_in_bytes, byteorder="big")
        return int_val

    def read_from_disk(self, path_to_page: str, column: int) -> bool:
        """
        Reads Pages from disk at the given file path
        """

        bin_file = open(path_to_page, "rb")
        bin_file.seek(column * PAGE_SIZE)
        self.data = bytearray(bin_file.read(PAGE_SIZE))
        bin_file.close()

        return True


def write_to_disk(path_to_page: str, all_columns: list):
    """
    Writes Pages to disk at the given file path
    """
    # print(f'Writing to {path_to_page}')
    bin_file = open(path_to_page, "wb")
    for i in range(len(all_columns)):
        bin_file.write(all_columns[i].data)
    bin_file.close()


def is_page_full(page: Page) -> bool:
    """
    Given a Page object this function will look at the total elements a page can hold: PAGE_SIZE/PAGE_RECORD_SIZE
    in config.py, then return True if the record count for the page is equal to that value and False otherwise.
    :param page: Page       Page object to check
    :return: bool           Returns True if page is full and False otherwise
    """

    total_elements_possible = (PAGE_SIZE / PAGE_RECORD_SIZE)  # How many total records the page can hold
    if page.num_records == total_elements_possible:           # If the record count is equal to the total possible
        return True                                           # the page is full
    return False
