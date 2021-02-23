from lstore.db import *
from lstore.table import *
from lstore.config import *
from lstore.query import *
from lstore.helpers import *

help_menu = """OPTIONS:
    exit: exit the program
    table: view table columns
    insert [student ID] [Grade 1] [Grade 2] [Grade 3]: insert record to database
    select [student ID] [col] [col0,col1,col2,col3]: find record with key Student ID in table
    delete [student ID]: delete record
    update [student ID] [col0,col1,col2,col3]: update record
    sum [start] [end] [column to sum]: sum the elements of column to sum betweent the ranges
    update [Student ID] [col0] [col1] [col2]: update record"""

def run_demo():
    demo_db = Database()
    demo_db.create_table(name='Students', num_columns=4, key=0)
    demo_table = demo_db.get_table("Students")
    demo_query = Query(demo_table)

    while (1):
        usr_in = input("ecs165.m1 > ")
        commands = usr_in.split(' ')  

        if commands[0] == "exit":
            pkl("exiting database...")
            break
        elif commands[0] == "table":
            pkl(f'Table "{demo_table.name}" at key {demo_table.key} with {demo_table.num_columns} columns')
        elif commands[0] == "insert":
            record = []
            for i in commands[1:]:
                record.append(int(i))
            exists = demo_table.record_does_exist(record[0])
            '''if exists != None:
                pkl("Record already exists")
                continue'''

            q = demo_query.insert(record[0], record[1], record[2], record[3])
            if q:
                pkl("successfully inserted", record)
            else:
                pkl("failed to insert", record)
        elif commands[0] == "select":
            sid = int(commands[1])
            col = int(commands[2])
            q_cols = [int(i) for i in commands[3][1:-1].split(',')]
            ret_record = demo_query.select(sid, col, q_cols)
            if ret_record == False:
                pkl("Could not find record")
            else:
                pkl("Found record with data", ret_record[0].user_data)
        elif commands[0] == "delete":
            sid = int(commands[1])
            
            demo_delete = demo_query.delete(sid)
            if demo_delete:
                pkl("Successfully deleted", sid)
            else:
                pkl("failed to delete", sid)
        elif commands[0] == "sum":
            start = int(commands[1])
            end = int(commands[2])
            col_index = int(commands[3])
            demo_sum = demo_query.sum(start, end, col_index)
            if (demo_sum == False):
                pkl("Could not calculate sum")
            else:
                pkl("Sum is", demo_sum)
        elif commands[0] == "update":
            sid = int(commands[1])
            updates = []
            for i in commands[2:]:
                if i == "None":
                    updates.append(None)
                else:
                    updates.append(int(i))
            ret = demo_query.update(sid, None, updates[0], updates[1], updates[2])
            if not ret:
                pkl("Failed to update")
            else:
                print(f'Updated {sid} with {updates}')
        elif commands[0] == "--help":
            print(help_menu)
        else:
            print("Invalid command")

    return

run_demo()