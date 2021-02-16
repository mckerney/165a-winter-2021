import math
from random import randint
import shutil

from lstore.db import *
from lstore.table import *
from lstore.config import *
from lstore.query import *


def makeTable():
    '''
    Test used to visualize Table layout
    '''
    
    x = Table('students', 5, 1)
    print(x)
    print(x.page_ranges)
    for pr in x.page_ranges:
        print('Page Range:\n')
        print(f'{pr}\n')
        for bp in x.page_ranges[0].base_pages:
            print('     Base Page:\n')
            print(f'        {bp}\n')
            print('             Columns:\n')
            for column in bp.columns_list:
                # print(f'{column} : {column.data}\n')
                print(f'            {column}\n')

#makeTable()

def testInsert():
    records = {}
    for i in range(0, 1000):
        key = 92106429 + randint(0, 9000)
        while key in records:
            key = 92106429 + randint(0, 9000)
        records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
        # query.insert(*records[key])
        # print('inserted', records[key])
    # print(records)
    print(*records[key])
    print(records[key])
# testInsert()

def __rid_to_page_location(rid: int) -> dict:
    #pr_index = 3 # len(self.book)
    # number of entries in a page range is 8192
    # number of entries in a base page is 512
    page_range_index = math.floor(rid / ENTRIES_PER_PAGE_RANGE)
    index = rid % ENTRIES_PER_PAGE_RANGE
    base_page_index = math.floor(index / ENTRIES_PER_PAGE)
    physical_page_index = index % ENTRIES_PER_PAGE
    return { 'page_range': page_range_index, 'base_page': base_page_index, 'page_index': physical_page_index }

# print(f'{__rid_to_page_location(2050)}\n')


def test_database() -> None:
    '''
    Test to check DB table creation functionality
    '''
    print('----------- test_database -------------')
    
    # check DB and Table creation
    test_db = Database()
    test_table = test_db.create_table(name='Students', num_columns=2, key=0)

    # Displaying general Table statistics
    print(f'Table: {test_table}\n')
    print(f'Table key : {test_table.key}\n')
    print(f'Table name : {test_table.name}\n')
    print(f'Table columns: {test_table.num_columns}\n')
    print(f'Table records : {test_table.num_records}\n')
    print(f'Table Page Ranges : {len(test_table.page_ranges)}\n')
    table_bp = 0
    for i in range(len(test_table.page_ranges)):
        table_bp += len(test_table.page_ranges[i].base_pages)
        print(f'PR_{i} Base Pages : {len(test_table.page_ranges[i].base_pages)}\n')
    print(f'Table total Base Pages: {table_bp}\n')
    print(f'Table column names: {[test_table.column_names.get(key) for key in test_table.column_names]}\n')
    
    # Add new Page Range
    print('------ Page Range -----')
    new_pr_key = test_table.create_new_page_range()
    print(f'New PR key: {new_pr_key}')
    table_bp_2 = 0
    for i in range(len(test_table.page_ranges)):
        table_bp_2 += len(test_table.page_ranges[i].base_pages)
        print(f'PR_{i} Base Pages : {len(test_table.page_ranges[i].base_pages)}\n')
    print(f'Table total Base Pages: {table_bp_2}\n')
    pass

#test_database()

def clear_database():
    shutil.rmtree("./root")

def test_database2() -> None:
    '''
    Test to check DB table creation functionality
    '''
    print('----------- test_database2 -------------')
    
    # check DB and Table creation
    # clear_database()
    db = Database()
    db.open("./root")
    student_table = db.create_table(name='Students', num_columns=6, key=0)
    teacher_table = db.create_table(name='Teachers', num_columns=3, key=1)
    student_query = Query(student_table)
    print(student_query)
    student_query.insert(7909887, 1, 2, 3, 4, 5)
    student_query.insert(8798797, 6, 7, 8, 9, 10)
    db.close()

# print(test_database2())

def test_insert_read() -> None:
    '''
    Test to check insert and read from the bufferpool
    '''
    print('----------- test_insert_read -------------')
    # clear_database()
    db = Database()
    db.open('./root')
    test_table = db.create_table('test', 6, 0)
    query = Query(test_table)
    query.insert(999, 1, 2, 3, 4, 5)
    record = query.select(999, 0, [1,1,1,1,1,1])
    print(record[0].all_columns)
    # db.drop_table('test')
    db.close()

test_insert_read()