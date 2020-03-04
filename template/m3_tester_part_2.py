from template.db import Database
from template.query import Query
from template.transaction import Transaction
from template.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed

db = Database()
db.open('/home/pkhorsand/165a-winter-2020-private/db')
grades_table = db.get_table('Grades')

keys = []
records = {}
seed(3562901)
num_threads = 8

# generate records already in database
for i in range(0, 1000):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

transaction_workers = []
select_transactions = []
update_transactions = []
for i in range(num_threads):
    select_transactions.append(Transaction())
    update_transactions.append(Transaction())
    transaction_workers.append(TransactionWorker())
    transaction_workers[i].add_transaction(select_transactions[i])
    transaction_workers[i].add_transaction(update_transactions[i])
worker_keys = [ {} for worker in transaction_workers ]

t = 0
_records = [records[key] for key in keys]
for c in range(grades_table.num_columns):
    _keys = list(set([record[c] for record in _records]))
    index = {v: [record for record in _records if record[c] == v] for v in _keys}
    for key in _keys:
        found = False
        for j, key_set in enumerate(worker_keys):
            if j != t % num_threads and any(map(lambda r: r[0] in key_set, index[key])):
                found = True
                break
        if not found:
            query = Query(grades_table)
            select_transactions[t % num_threads].add_query(query.select, key, c, [1, 1, 1, 1, 1])
            worker_keys[t % num_threads].update({record[0]: True for record in index[key]})
        t += 1

t = 0
for j in range(0, num_threads):
    for key in worker_keys[j]: 
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns):
            value = randint(0, 20)
            updated_columns[i] = value
            records[key][i] = value
            query = Query(grades_table)
            update_transactions[t % num_threads].add_query(query.update, key, *updated_columns)
            t += 1
            updated_columns = [None, None, None, None, None]

for transaction_worker in transaction_workers:
    transaction_worker.run()

score = len(keys)
for key in keys:
    correct = records[key]
    query = Query(grades_table)
    #TODO: modify this line based on what your SELECT returns
    result = query.select(key, 0, [1, 1, 1, 1, 1])[0].columns
    if correct != result:
        print('select error on primary key', key, ':', result, ', correct:', correct)
        score -= 1
print('Score', score, '/', len(keys))

db.close()
