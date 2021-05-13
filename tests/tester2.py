from lstore.db import Database
from lstore.query import Query
from lstore.config import *
from lstore.index import *
from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

grades_table = db.get_table('Grades')
query = Query(grades_table)

# repopulate with random data
records = {}
seed(3562901)
for i in range(0, 1000):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

# Simulate updates
keys = sorted(list(records.keys()))
for _ in range(10):
    for key in keys:
        for j in range(1, grades_table.num_columns):
            value = randint(0, 20)
            records[key][j] = value
keys = sorted(list(records.keys()))
for key in keys:
    print(records[key])
    print(records[key])

for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    print('selected',key)
    error = False
    for i, column in enumerate(record.user_data):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
print("Select finished")

print('----- test create -----')
# create Index
main_index = Index(grades_table)
main_index.create_default_primary_index() # create index for column 0
print("Created Index of size", len(main_index.indices[0].index))
main_index.create_index(1)  # create index for column 1
print("Created Index of size", len(main_index.indices[1].index))

# test that indices have the correct number of elements
counter = 0
for key in keys:
    index_records = main_index.indices[0].get(key)
    if (index_records == None):
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        if record:
            print(f'Record {key} expected at position {counter} but found None')
    counter += 1

print('----- test get -----')
col_one_records_sum = 0
for i in range(0, 21):
    index_records = main_index.indices[1].get(i)
    num_records = len(index_records)
    col_one_records_sum += num_records
    print(f'There are {num_records} records with col 1 value of {i}')
print(f'A total of {col_one_records_sum} records were found via the column 1 index')

print('----- test update -----')
# test updating from an index
count = 0
for i in range(0, 21):
    rids = main_index.indices[1].get(i)
    for j in rids:
        main_index.indices[1].update(randint(0, 20), i, j)

col_one_records_sum = 0
for i in range(0, 21):
    index_records = main_index.indices[1].get(i)
    num_records = len(index_records)
    col_one_records_sum += num_records
    print(f'There are {num_records} records with col 1 value of {i}')
print(f'A total of {col_one_records_sum} records were found via the column 1 index')

print('----- test delete -----')
# delete all rids using column one
for i in range(0, 21):
    rids = main_index.indices[1].get(i)
    for j in rids:
        main_index.indices[1].delete(i, j)

col_one_records_sum = 0
for i in range(0, 21):
    rids = main_index.indices[1].get(i)
    num_records = len(rids)
    col_one_records_sum += num_records
    print(f'There are {num_records} records with col 1 value of {i}')

db.close()
