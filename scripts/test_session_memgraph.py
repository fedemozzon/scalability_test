import random
from gqlalchemy import Memgraph
import pandas as pd
from typing import Dict

import logging
from random import randint
from time import perf_counter
import concurrent.futures

THREADS_SIZE = 7

def thread_function(name):
    t1_start = perf_counter()
    logging.info("Thread %s: starting", name)

    # Create a node with the label FirstNode and message property with the value "Hello, World!"
    query = """CREATE (n:FirstNode)
           SET n.message = '{message}'
           RETURN 'Node '  + id(n) + ': ' + n.message AS result""".format(message="Hello, World from node "+str(name)+"!")

    # Execute the query
    results = memgraph.execute_and_fetch(query)
    print(list(results)[0]['result'])
    t1_stop = perf_counter()
    logging.info("Thread %s: finishing", name)

memgraph = Memgraph(host='127.0.0.1', port=7687)
format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS_SIZE) as executor:
        executor.map(thread_function, range(THREADS_SIZE))

