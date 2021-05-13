from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.helpers import *
from lstore.config import *
from random import choice, randint, sample, seed

clear_database('./ECS165')

db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)

keys = []
records = {}
seed(3562901)
num_execution_threads = QUEUES_PER_GROUP

# Create Indices
try:
    grades_table.index.create_index(1)
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

insert_transactions = []
select_transactions = []
update_transactions = []

for i in range(num_execution_threads):
    insert_transactions.append(Transaction())
    select_transactions.append(Transaction())
    update_transactions.append(Transaction())

# Instantiate a Query object for the Grades Table
query = Query(grades_table)

# Test Insert
num_inserts = 1000
print(f"TESTING {num_inserts} INSERTS")
for i in range(0, num_inserts):
    key = 92106429 + i
    keys.append(key)
    i = i % num_execution_threads
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    insert_transactions[i].add_query(query.insert(*records[key]))

for i in range(num_execution_threads):
    db.batcher.enqueue_xact(insert_transactions[i])

# Let inserts complete
db.let_execution_threads_complete()
success = 0
for completed_transaction in insert_transactions:
    for result in completed_transaction.results:
        if result:
            success += 1

print(f"{success} / {num_inserts} RECORDS SUCCESSFULLY INSERTED\n")

# Test Select on Multiple Columns using Indices
print(f"SELECTING RECORDS FOR ALL COLUMNS USING CREATED INDEX")
t = 0
_records = [records[key] for key in keys]
for c in range(grades_table.num_columns):
    _keys = sorted(list(set([record[c] for record in _records])))
    index = {v: [record for record in _records if record[c] == v] for v in _keys}

    for key in _keys:
        select_transactions[t % num_execution_threads].add_query(query.select(key, c, [1, 1, 1, 1, 1]))

    t += 1

for i in range(num_execution_threads):
    db.batcher.enqueue_xact(select_transactions[i])

db.let_execution_threads_complete()
success = 0
for completed_transaction in select_transactions:
    for result in completed_transaction.results:
        for record in result:
            if record != False:
                success += 1

print(f"{success} / {t*num_inserts} RECORDS SUCCESSFULLY SELECTED\n")

# Test Update
num_updates = num_inserts*(grades_table.num_columns-1)*num_execution_threads
print(f"TESTING {num_updates} UPDATES")
for j in range(num_execution_threads):
    for key in keys:
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns):
            value = randint(0, 20)
            updated_columns[i] = value
            records[key][i] = value
            q_op = query.update(key, *updated_columns)
            update_transactions[j].add_query(q_op)
            updated_columns = [None, None, None, None, None]

for i in range(num_execution_threads):
    db.batcher.enqueue_xact(update_transactions[i])

# Let all operations complete before the final select
db.let_execution_threads_complete()
success = 0
for completed_transaction in update_transactions:
    for result in completed_transaction.results:
        if result:
            success += 1

print(f"{success} / {num_updates} RECORDS SUCCESSFULLY UPDATED\n")

print(f"SELECTING {num_inserts} RECORDS AFTER UPDATES")
score = len(keys)
for key in keys:
    correct = records[key]
    query = Query(grades_table)
    q_op = query.select(key, 0, [1, 1, 1, 1, 1])
    result_list = q_op.run()
    result = result_list[0]
    if correct != result:
        print('select error on primary key', key, ':', result, ', correct:', correct)
        score -= 1
print('Score', score, '/', len(keys))

db.close()

