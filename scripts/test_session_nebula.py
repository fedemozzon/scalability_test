import random
import threading

from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import pandas as pd
import datetime

import logging
from random import randint
from time import perf_counter
import concurrent.futures

# CONSTANTES

#DB
DB_IP = '127.0.0.1'
DB_PORT = 9669

query_size = 10
array_querys = [query_size, query_size]
THREADS_SIZE = 7
QUERY_READ = "MATCH (n) RETURN n LIMIT 100;"
QUERY_WRITE = f'INSERT VERTEX users(name, email) VALUES {randint(10000, 500000)}:("John", "john@example.com");'

# Create a lock
lock = threading.Lock()

config = Config()
connection_pool = ConnectionPool()


# # option 2 with session_context, session will be released automatically
def nebula_connection(thread_num, query):
    try:
        with connection_pool.session_context('root', 'root') as session:
            # print("CONNECTING TO NEBULA THREAD " + str(thread_num))
            session.execute('USE space1')

            # Aca poner la query que se nos cante
            result = session.execute(query)

            connection_pool.close()
    except Exception as e:
        print(e)
        connection_pool.close()


def thread_function(name):
    t1_start = perf_counter()
    logging.info("Thread %s: starting", name)
    query = random.choice([QUERY_READ, QUERY_WRITE])
    nebula_connection(name, query)

    # restar lock
    if query == QUERY_READ:
        with lock:
            array_querys[0] -= 1
    else:
        with lock:
            array_querys[1] -= 1

    t1_stop = perf_counter()
    logging.info(
        f'El thread {name} ejecutó la query de {"READ" if query == QUERY_READ else "WRITE"} en {t1_stop - t1_start}')
    logging.info("Thread %s: finishing", name)

    # Save info into a csv file
    df = pd.DataFrame([["READ" if query == QUERY_READ else "WRITE", datetime.datetime.now(), name, t1_stop - t1_start]],
                      columns=['Operation', 'time', 'id_thread', 'time [s]'])
    df.to_csv('nebula.csv', index=False, mode='a', header=False)


# VERIFICAR CONEXION
try:
    connection_pool.init([(DB_IP, DB_PORT)], config)
except:
    print("no se pudo conectar, verificar que al menos un container este corriendo")
    exit()


# crear csv con headers operation, time, thread, time_finish
with open('memgraph.csv', 'w') as f:
    f.write('operation,time,thread,time_finish\n')


while array_querys[0] > 0 or array_querys[1] > 0:
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS_SIZE) as executor:
        executor.map(thread_function, range(THREADS_SIZE))
