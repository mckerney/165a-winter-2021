from lstore.config import * 
from lstore.page import *


class Bufferpool:

    def __init__(self, path_to_root):
        self.frames = []
        self.frame_directory = {}  # (table_name, page_range, base_or_tail_page_index, is_base_record) : frame index
        self.frame_count = 0
        self.path_to_root = path_to_root

    def _add_frame_to_directory(self, table_name, page_range, base_page, is_base_record, frame_index):
        new_frame_key = (table_name, page_range, base_page, is_base_record)
        self.frame_directory[new_frame_key] = frame_index
        self.frames[frame_index].tuple_key = new_frame_key
        
        if self.frame_count < BUFFERPOOL_FRAME_COUNT:
            self.frame_count += 1

    def at_capacity(self):
        if self.frame_count == BUFFERPOOL_FRAME_COUNT:
            return True
        
        return False

    def is_record_in_pool(self, table_name, record_info: dict) -> bool:
        """
        Function that checks if a record exists in the Bufferpool
        """

        is_base_record = record_info.get("is_base_record")
        page_range = record_info.get("page_range")

        if is_base_record:
            base_page_index = record_info.get("base_page")
        else:
            base_page_index = record_info.get("tail_page")

        frame_info = (table_name, page_range, base_page_index, is_base_record)

        if frame_info in self.frame_directory:
            return True

        return False

    def evict_page(self):
        """
        Function that evicts a page from the Bufferpool
        """

        least_used_page = float('inf')
        index = 0

        for frame in self.frames:
            if frame.access_count < least_used_page:
                least_used_page = index
            index += 1

        if self.frames[least_used_page].dirty_bit:
            frame_to_write = self.frames[least_used_page]
            write_path = self.frames[least_used_page].path_to_page_on_disk
            all_columns = frame_to_write.all_columns
            write_to_disk(write_path, all_columns)

        frame_key = self.frames[least_used_page].tuple_key
        # print(f'EVICTING {frame_key}')
        del self.frame_directory[frame_key]

        return least_used_page

    def load_page(self, table_name: str, num_columns: int, page_range_index: int, base_page_index: int,
                  is_base_record: bool):
        """
        Function that loads a page into the Bufferpool
        """
        # Check whether this is a base record or a tail record
        if is_base_record:
            path_to_page = f"{self.path_to_root}/{table_name}/page_range_{page_range_index}/" \
                           f"base_page_{base_page_index}.bin"
        else:
            path_to_page = f"{self.path_to_root}/{table_name}/page_range_{page_range_index}/" \
                           f"tail_pages/tail_page_{base_page_index}.bin"

        # the frame index will be the count if the bufferpool is not at capacity

        # need to evict a page because the bufferpool is at capacity
        if self.at_capacity():
            frame_index = self.evict_page()
            self.frames[frame_index] = Frame(path_to_page_on_disk=path_to_page, table_name=table_name)
        else:
            frame_index = self.frame_count
            self.frames.append(Frame(path_to_page_on_disk=path_to_page, table_name=table_name))

        # Pin the frame
        self.frames[frame_index].pin_frame()

        # Allocate physical pages for meta data and user data
        self.frames[frame_index].all_columns = [Page(column_num=i) for i in range(num_columns + META_COLUMN_COUNT)]

        # Read in values from disk
        # print(f'Reading from {path_to_page}')
        for i in range(num_columns + META_COLUMN_COUNT):
            self.frames[frame_index].all_columns[i].read_from_disk(path_to_page=path_to_page, column=i)

        # Add the frame to the frame directory
        self._add_frame_to_directory(table_name, page_range_index, base_page_index, is_base_record, frame_index)

        return frame_index

    def commit_page(self, frame_index):
        frame_to_commit = self.frames[frame_index]
        all_columns = frame_to_commit.all_columns
        path_to_page = frame_to_commit.path_to_page_on_disk
        if frame_to_commit.dirty_bit:
            write_to_disk(path_to_page, all_columns)

    def commit_all_frames(self):
        for i in range(len(self.frames)):
            if self.frames[i].dirty_bit:
                self.commit_page(frame_index=i)
                

class Frame:

    def __init__(self, path_to_page_on_disk, table_name):
        self.all_columns = []   # Initialize at none since different tables have different column counts
        self.dirty_bit = False
        self.pin = False
        self.time_in_bufferpool = 0
        self.access_count = 0   # number of times page has been accessed
        self.path_to_page_on_disk = path_to_page_on_disk
        self.table_name = table_name
        self.tuple_key = None

    def set_dirty_bit(self):
        self.dirty_bit = True
        return True

    def pin_frame(self):
        self.pin = True
        return True

    def unpin_frame(self):
        self.pin = False
        return True
