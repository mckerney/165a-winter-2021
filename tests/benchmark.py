from lstore.db import Database
from lstore.query import Query
from lstore.helpers import *
from time import process_time
from random import choice, randrange
import pickle

def benchmark():
    # Student Id and 4 grades
    res = {}
    db = Database()
    clear_database('./root')
    db.open('./root')
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)
    keys = []

    insert_time_0 = process_time()
    for i in range(0, 10000):
        query.insert(906659671 + i, 93, 0, 0, 0)
        keys.append(906659671 + i)
    insert_time_1 = process_time()

    insert_tot = insert_time_1 - insert_time_0
    res['insert'] = insert_tot
    print("Inserting 10k records took:  \t\t\t", insert_tot)


    # Measuring update Performance
    update_cols = [
        [randrange(0, 100), None, None, None, None],
        [None, randrange(0, 100), None, None, None],
        [None, None, randrange(0, 100), None, None],
        [None, None, None, randrange(0, 100), None],
        [None, None, None, None, randrange(0, 100)],
    ]

    update_time_0 = process_time()
    for i in range(0, 10000):
        query.update(choice(keys), *(choice(update_cols)))
    update_time_1 = process_time()

    update_tot = update_time_1 - update_time_0
    res['update'] = update_tot
    print("Updating 10k records took:  \t\t\t", update_tot)

    # Measuring Select Performance
    select_time_0 = process_time()
    for i in range(0, 10000):
        query.select(choice(keys),0 , [1, 1, 1, 1, 1])
    select_time_1 = process_time()

    select_tot = select_time_1 - select_time_0
    res['select'] = select_tot
    print("Selecting 10k records took:  \t\t\t", select_tot)

    # Measuring Aggregate Performance
    agg_time_0 = process_time()
    for i in range(0, 10000, 100):
        result = query.sum(i, 100, randrange(0, 5))
    agg_time_1 = process_time()

    agg_tot = agg_time_1 - agg_time_0
    res['agg'] = agg_tot
    print("Aggregate 10k of 100 record batch took:\t\t", agg_tot)

    # Measuring Delete Performance
    delete_time_0 = process_time()
    for i in range(0, 10000):
        query.delete(906659671 + i)
    delete_time_1 = process_time()

    delete_tot = delete_time_1 - delete_time_0
    res['delete'] = delete_tot
    print("Deleting 10k records took:  \t\t\t", delete_tot)
    db.close()
    return res

LOOPS = 10

aggregate_insert = 0
aggregate_update = 0 
aggregate_select = 0
aggregate_sum = 0
aggregate_delete = 0

for i in range(LOOPS):
    res = benchmark()
    aggregate_insert += res['insert']
    aggregate_update += res['update']
    aggregate_select += res['select']
    aggregate_sum += res['agg']
    aggregate_delete += res['delete']

print('average insert time for 10k records:', aggregate_insert/LOOPS)
print('average update time for 10k records:', aggregate_update/LOOPS)
print('average select time for 10k records:', aggregate_select/LOOPS)
print('average sum time for 10k records:', aggregate_sum/LOOPS)
print('average delete time for 10k records:', aggregate_delete/LOOPS)

