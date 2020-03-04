from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transction

from random import choice, randint, sample, seed

db = Database()
db.open('~/ECS165')
# Student Id and 4 grades
grades_table = db.create_table('Grades', 5, 0)

try:
    grades_table.index.create_index(1)
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not imlemented properly, tests may fail.')

seed(3562901)
records = {}
keys = []
insert_transactions = []
# Insert phase
for i in range(24):
    insert_transactions.append(Transaction())

for i in range(0, 10000):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    q = Query(grades_table)
    t = insert_transactions[i % 24]
    t.add_query(q.insert, *records[key])

# run these transactions
