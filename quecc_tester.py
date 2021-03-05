from lstore.db import Database
from lstore.query import Query
from lstore.transaction import *
from lstore.helpers import *
import threading

def m3_tester():
    path = './MS3'
    clear_database(path)
    db = Database()
    db.open('./MS3')
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)

    transaction = Transaction()
    query_1 = query.insert(*[1,2,3,4,5])
    print("Query 1", query_1.timestamp)
    query_2 = query.select(1,0,[1, 1, 1, 1, 1])
    print("Query 2", query_2.timestamp)

    transaction.queries.append(query_1)
    transaction.queries.append(query_2)

    # Do i use the batcher in db or table
    db.batcher.enqueue_xact(transaction)
    db.batcher.batch_xact()
    #db.close()

main_thread = threading.Thread(target=m3_tester)
main_thread.start()
#main_thread.join()
