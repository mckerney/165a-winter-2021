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

    transaction1 = Transaction()
    query_1 = query.insert(*[1,2,3,4,5])
    query_2 = query.select(1,0,[1, 1, 1, 1, 1])
    query_3 = query.insert(*[6, 7, 8, 9, 10])


    transaction1.queries.append(query_1)
    transaction1.queries.append(query_2)
    transaction1.queries.append(query_3)

    db.batcher.enqueue_xact(transaction1)

    transaction2 = Transaction()
    query_3 = query.insert(*[6, 7, 8, 9, 10])
    transaction2.queries.append(query_3)
    db.batcher.enqueue_xact(transaction2)

    db.batcher.batch_xact()
    db.close()

main_thread = threading.Thread(target=m3_tester)
main_thread.start()

