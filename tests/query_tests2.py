from lstore.query import *
from lstore.table import *
from lstore.db import *
from lstore.helpers import *

def test_select():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    did_insert = query.insert(798329, 1, 2, 3, 4, 5)
    ret_record = query.select(798329, 0, [1, 1, 1, 1, 0, 1])
    print(ret_record[0].user_data)
    # print(ret_record)

test_select()

def test_insert():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    did_insert = query.insert(999, 1, 2, 3, 4, 6)
    print("did_insert:" , did_insert)
    did_insert2 = query.insert(998, pow(2,64)-1, 2, 3, 4, 5)
    did_insert3 = query.insert(999, 3, 8, 3, 4, 5)
    did_insert4 = query.insert(9809090, 3, 8, -1, 4, 5)
    print("did_insert:" , did_insert)
    print("did_insert2:" , did_insert2)
    print("did_insert3:" , did_insert3)
    print("did_insert4:" , did_insert4)
    print(query.table.num_records)

test_insert()

def test_insert_and_see_if_exists():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    key = 888887980
    did_insert = query.insert(key, 1, 2, 3, 4, 5)
    did_insert2 = query.insert(53245, 2,3,4,5,6)
    print("did_insert:", did_insert)
    print("did_insert2:", did_insert2)
    ret_value = query.table.record_does_exist(key)
    ret_value2 = query.table.record_does_exist(53245)
    print("ret_value:", ret_value)
    print("ret_value2:", ret_value2)

test_insert_and_see_if_exists()

def test_insert_and_delete_and_see_if_exists():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    key = 888887980
    key2 = 53245
    did_insert = query.insert(key, 1, 2, 3, 4, 5)
    did_insert2 = query.insert(key2, 2,3,4,5,6)
    print("did_insert:", did_insert)
    did_delete = query.delete(key)
    did_delete2 = query.delete(key2)
    print("did_delete", did_delete)
    print("did_delete2", did_delete2)
    ret_value = query.table.record_does_exist(key)
    ret_value2 = query.table.record_does_exist(key2)
    print("ret_value:", ret_value)
    print("ret_value2:", ret_value2)

test_insert_and_delete_and_see_if_exists()

def test_insert_and_read():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    key = 888887980
    did_insert = query.insert(key, 1, 2, 3, 4, 5)
    validRID = table.record_does_exist(key)
    did_read = table.read_record(validRID)
    print("did_read:", did_read.all_columns)

test_insert_and_read()

def test_get_and_set_bit():
    zero = 0
    schema_encoding = zero.to_bytes(8, "big")
    schema_encoding_int = int.from_bytes(schema_encoding, "big")
    getBit = get_bit(value=schema_encoding_int, bit_index=5)
    print("get 1", getBit)
    setBit = set_bit(value=schema_encoding_int,bit_index=5)
    print("set bit", setBit)
    getBit = get_bit(value=setBit, bit_index=7)
    print("get 2", getBit)

test_get_and_set_bit()

def test_insert_and_read_and_update():
    print('START test_insert_and_read_and_update()')
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    key = 888887980
    print('---- First Record -----')
    did_insert = query.insert(key, 1, 2, 3, 4, 5)
    print('Inserted with [key, 1, 2, 3, 4, 5]')
    ret = query.update(key, None, 7, 8, 9, 10, None)
    print('Updated with [None, 7, 8, 9, 10, None]')
    ret_2 = query.select(key, 0, [1,1,1,1,1,1])
    print('Return 2, col ', ret_2[0].user_data)
    ret_3 = query.update(key, None, 13, None, 20000, None, None)
    print('Updated with [None, 13, None, 20000, None, None]')
    ret_4 = query.select(key, 0, [1,1,1,1,1,1])
    print('Return 4, col ', ret_4[0].user_data)
    ret_5 = query.update(key, None, 990907, 4, 36, 1800000000, 57)
    print('Updated with [None, 990907, 4, 36, 1800000000, 57]')
    ret_6 = query.select(key, 0, [1,1,1,1,1,1])
    print('Return 6, col ', ret_6[0].user_data)
    print('---- Second Record -----')
    key_2 = 888887981
    inserted = query.insert(key_2, 10, 11, 12, 13, 14)
    print('Inserted with [10, 11, 12, 13, 14]')
    upd_1 = query.update(key_2, None, 20, 21, None, None, 24)
    print('Updated with [20, 21, None, None, 24]')
    sel_1 = query.select(key_2, 0, [1,1,1,1,1,1])
    print('Select 1, col', sel_1[0].user_data)
    upd_2 = query.update(key_2, None, None, None, 32, 33, None)
    print('Updated with [None, None, None, 32, 33, None]')
    sel_2 = query.select(key_2, 0, [1,1,1,1,1,1])
    print('Select 2, col', sel_2[0].user_data)
    print('---- Third Record -----')
    key_3 = 99999
    inserted = query.insert(key_3, 10, 11, 12, 13, 14)
    print('Inserted with [10, 11, 12, 13, 14]')
    # upd_1 = query.update(key_3, None, 20, 21, None, None, 24)
    # print('Updated with [20, 21, None, None, 24]')
    sel_1 = query.select(key_3, 0, [1,1,1,1,1,1])
    print('Select 1, col', sel_1[0].user_data)
    # upd_2 = query.update(key_3, None, None, None, 32, 33, None)
    # print('Updated with [None, None, None, 32, 33, None]')
    sel_2 = query.select(key_3, 0, [1,1,1,1,1,1])
    print('Select 2, col', sel_2[0].user_data)
    print('END test_insert_and_read_and_update()')
    #test_insert_and_read_and_update()
    print("did insert", did_insert)
    ret = query.update(key, None, 0, 1, 2, 3, None)
    print(ret)

test_insert_and_read_and_update()

def test_insert_and_read_and_update2():
    print('START test_insert_and_read_and_update()')
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    key = 8888
    key2 = 9999
    did_insert = query.insert(key, 1, 2, 3, 4, 5)
    did_insert2 = query.insert(key2, 6, 7, 8, 9, 10)
    did_update = query.update(key, 889, 11, None, None, None, None)
    did_update = query.update(key, None, None, None, None, 44, None)
    did_update = query.update(key, None, None, None, None, None, 55)
    did_update = query.update(key, None, None, 22, None, None, None)
    did_select = query.select(key, 0, [1, 1, 1, 1, 1, 1])
    print(did_select[0].all_columns)

test_insert_and_read_and_update2()

database = Database()
database.create_table("Students", 6, 0)
table = database.get_table("Students")
query = Query(table)
for i in range(1, 20):
    did_insert0 = query.insert(i, 0, 2, 3, 4, 5)

for i in range(1, 20):
    did_update0 = query.update(i, None, None, None, None, None, 20)

for i in range(1, 20):
    did_select = query.select(i, 0, [1, 1, 1, 1, 1, 1])
    print(did_select[0].user_data)

