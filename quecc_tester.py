from lstore.db import Database
from lstore.query import Query
from lstore.helpers import *
import threading

def m3_tester():
    path = './MS3'
    clear_database(path)
    db = Database()
    db.open('./MS3')
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)

    query.insert(*[1,2,3,4,5])
    output = query.select(1,0,[1, 1, 1, 1, 1])
    #db.close()

main_thread = threading.Thread(target=m3_tester)
main_thread.start()
#main_thread.join()
