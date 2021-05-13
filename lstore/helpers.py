import shutil


def get_bit(value, bit_index) -> int:
    some_or_none = value & (1 << bit_index)
    if some_or_none:
        return 1

    return 0


def set_bit(value, bit_index) -> int:
    return value | (1 << bit_index)


def get_size(fileobject):
    fileobject.seek(0, 2)  # move the cursor to the end of the file
    size = fileobject.tell()
    return size


def clear_database(path):
    shutil.rmtree(path)
