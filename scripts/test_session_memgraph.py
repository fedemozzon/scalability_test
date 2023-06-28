import random
import threading

from gqlalchemy import Memgraph
import pandas as pd
import datetime

import logging
from time import perf_counter
import concurrent.futures

# CONSTANTES

# DB
print("---STARTING---")
try:
    memgraph = Memgraph(host='127.0.0.1', port=7687)
    memgraph._get_cached_connection().is_active()
except:
    print("Error al conectar con Memgraph. Verificar que esté corriendo.")
    exit()

# Create a lock
lock = threading.Lock()

# VARIOS
query_size = 10
array_querys = [query_size, query_size]
THREADS_SIZE = 10

### QUERIES

# Create a node with the label FirstNode and message property with the value "Hello, World!"
query_write = lambda name: """CREATE (n:FirstNode)
        SET n.message = '{message}'
        RETURN 'Node '  + id(n) + ': ' + n.message AS result""".format(
    message="Hello, World from node " + str(name) + "!")

# query read simple
query_read = """MATCH (n:FirstNode)
        RETURN 'Node '  + id(n) + ': ' + n.message AS result"""


def thread_function(name):
    t1_start = perf_counter()
    logging.info("Thread %s: starting", name)

    # choose random query
    query = query_read if random.randint(0, 1) == 0 else query_write(
        name)  # Cuidado aca el write va con () porque es una funcion que devuelve la query
    query_type = 'READ' if query == query_read else 'WRITE'

    # Execute the query
    results = memgraph.execute_and_fetch(query)
    print(list(results)[0]['result'])

    # restar lock
    if query == query_read:
        with lock:
            array_querys[0] -= 1
    else:
        with lock:
            array_querys[1] -= 1

    t1_stop = perf_counter()
    logging.info(f'El thread {name} ejecutó la query de {query_type} en {t1_stop - t1_start}')
    logging.info("Thread %s: finishing", name)

    # Save info into a csv file
    df = pd.DataFrame([[query_type, datetime.datetime.now(), name, t1_stop - t1_start]],
                      columns=['Operation', 'time', 'id_thread', 'time [s]'])
    df.to_csv('memgraph.csv', index=False, mode='a', header=False)


while array_querys[0] > 0 or array_querys[1] > 0:
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS_SIZE) as executor:
        executor.map(thread_function, range(THREADS_SIZE))
