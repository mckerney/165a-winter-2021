from lstore.query import *
from lstore.table import *
from lstore.db import *

def test_select():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    ret_record = query.select(798329, 0, [0, 1, 0, 1, 0, 1])
    print(ret_record)

# test_select()

def test_insert():
    database = Database()
    database.create_table("Students", 6, 0)
    table = database.get_table("Students")
    query = Query(table)
    did_insert = query.insert(999, 1, 2, 3, 4, 5)

test_insert()