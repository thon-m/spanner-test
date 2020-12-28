from google.cloud import spanner
import uuid
import random
import time

spanner_client = spanner.Client()
instance = spanner_client.instance("bennet-spanner-inst")
database = instance.database("bennet-db")

select_template = "SELECT id, name, age FROM singers WHERE id = '{}'"

def insert_singer(txn, values):
    txn.insert(
	'singers',
	columns=['id', 'name', 'age'],
	values=values
    )

def select_singer(txn, pk):
    #txn.read(
    #    table = "singers",
    #    columns=['id', 'name', 'age'],
    #    keyset=[pk]
    #)
    results = txn.execute_sql(
        "SELECT id, name, age FROM singers WHERE id = '{}'".format(pk)
    )
    #for row in results:
    #    print(u"id: {}, name: {}, age: {}".format(*row))
    return results
        

def update_singer(txn, pk, name):
    result = txn.execute_update(
        "UPDATE singers SET name='{}' WHERE id = '{}'".format(name, pk)
    )
    #print(result)
    return result

def insert_sample_data():
    for i in range(100,199):
        values = []
        for j in range(10000, 10999):
            values.append([str(i) + "_" + str(j), str(uuid.uuid4()), random.randint(0,99)])
        database.run_in_transaction(insert_singer, values)

def random_row():
    rand = random.randint(100,199)
    t = time.time_ns()
    return [str(rand) + str(t), str(t), rand]

def insert_test(total):
    count = 0
    for i in range(0, total):
        database.run_in_transaction(insert_singer, [random_row()])
        count = count + 1
    return count

def do_select_update_insert(txn, rows, insert_count):

    start = time.time_ns()

    for row in rows:
        select_singer(txn, row[0])
 
    select = time.time_ns()

    for row in rows:
        update_singer(txn, row[0], str(time.time_ns()))
 
    update = time.time_ns()

    for i in range(0,insert_count):
        row2 = random_row()
        insert_singer(txn, [row2])

    insert = time.time_ns()

    return [select-start, update - select, insert - update]

def select_update_insert(count, select_update_count, insert_count):
    total = [0,0,0,0,0]

    for i in range(1,count):
        rows = []
        for i in range(0,select_update_count):
            rows.append(random_row())

        database.run_in_transaction(insert_singer, rows)
        start = time.time_ns()
        result = database.run_in_transaction(do_select_update_insert, rows, insert_count)
        took = time.time_ns() - start
        total[0] = total[0] + 1
        total[1] = total[1] + result[0]
        total[2] = total[2] + result[1]
        total[3] = total[3] + result[2]
        total[4] = total[4] + took
  
    return total

def query_all(total):
    count = 0
    for i in range(0, total):
        group = random.randint(100,198)
        key = random.randint(10000,10998)
        pk = str(group) + "_" + str(key)

        with database.snapshot(multi_use=False) as snapshot:
            select_singer(snapshot, pk)
            #snapshot.read(
            #    table = "singers",
	    #    columns=['id', 'name', 'age'],
            #    keyset=[pk]
            #)
            #snapshot.execute_sql(select_template.format(pk))
            #database.run_in_transaction(lambda transaction: transaction.execute_sql(select_template.format(pk)))
        count = count + 1
    return count

def run_test_select(count):
    took = 0
    total = 0
    s = time.time_ns()
    #total = total + insert_test(1000)
    total = total + query_all(count)
    took = took + time.time_ns() - s
    avg = took / total
    print("single select by pk: total={}, took={}, avg={}, avg_ms={}".format(total, took, avg, avg / 1000000))

def run_test_insert(count):
    took = 0
    s = time.time_ns()
    total = insert_test(count)
    took = took + time.time_ns() - s
    avg = took / total
    print("insert one record: total={}, took={}, avg={}, avg_ms={}".format(total, took, avg, avg / 1000000))

def run_select_update_insert(count, select_update_count, insert_count):
    result = select_update_insert(count, select_update_count, insert_count)
    total = result[0]
    took = result[4]
    avg = took / total
    commit = took - result[1] - result[2] - result[3]
    print("{} x select, {} x update, {} x insert single txn: total={}, took={}, avg={}ms, avg_select={}ms, avg_update={}ms,avg_insert={}ms, avg_commit={}ms".format(
        select_update_count,
        select_update_count,
        insert_count,
        result[0], 
        took, 
        avg / 1000000,
        result[1] / total / 1000000,
        result[2] / total / 1000000,
        result[3] / total / 1000000,
        commit / total / 1000000
    ))

for i in range(0,10):
    run_test_select(100)
    run_test_insert(100)
    run_select_update_insert(100, 1, 1)
    run_select_update_insert(100, 2, 2)

