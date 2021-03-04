from lstore.db import Database
from lstore.query import Query
from lstore.helpers import *

clear_database('./MS3')

path = './MS3'
db = Database()
db.open('./MS3')

grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

query.insert(*[1,2,3,4,5])
# output = query.select(1,0,[1, 1, 1, 1, 1])
# print(f'select = {output[0].user_data}')

db.close()