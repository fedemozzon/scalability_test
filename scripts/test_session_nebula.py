import random
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import pandas as pd
import datetime
from typing import Dict
from nebula3.data.ResultSet import ResultSet

import logging
import time
from random import randint
from time import perf_counter
import concurrent.futures
# # Sarasa para convertir a un dataframe de pandas, por ahora no interesa
# def result_to_df(result: ResultSet) -> pd.DataFrame:
#     """
#     build list for each column, and transform to dataframe
#     """
#     assert result.is_succeeded()
#     columns = result.keys()
#     d: Dict[str, list] = {}
#     for col_num in range(result.col_size()):
#         col_name = columns[col_num]
#         col_list = result.column_values(col_name)
#         d[col_name] = [x.cast() for x in col_list]
#     return pd.DataFrame.from_dict(d, columns=columns)

# # define a config

# # init connection pool

# # if the given servers are ok, return true, else return false

# # option 2 with session_context, session will be released automatically
def nebula_connection(thread_num, query):
    config = Config()
    connection_pool = ConnectionPool()
    ok = connection_pool.init([('127.0.0.1', 9669)], config)
    if not ok:
        print("Connection failed")
        return
    try:
        with connection_pool.session_context('root', 'root') as session:
            # print("CONNECTING TO NEBULA THREAD " + str(thread_num))
            session.execute('USE space1')
            # print("USE SPACE space1")
            # Aca poner la query que se nos cante
            result = session.execute(query)
            # print(f"QUERY {query} EXECUTED")
            # df = result_to_df(result)
            # print(df)
            # print("RESULTS -->")
            # print(result)
            # connection_pool.close()
    except Exception as e:
        print(e)
        connection_pool.close()

#     # Capaz aca imprimir el resultado con un for o hacer algo

# # close the pool

THREADS_SIZE = 7

def thread_function(name):
    t1_start = perf_counter()
    logging.info("Thread %s: starting", name)
    query_read = "MATCH (n) RETURN n LIMIT 100;"
    query_write = f'INSERT VERTEX users(name, email) VALUES {randint(10000, 500000)}:("John", "john@example.com");'
    query = random.choice([query_read, query_write])
    nebula_connection(name, query)
    t1_stop = perf_counter()
    logging.info(f'El thread {name} ejecutó la query de {"READ" if query == query_read else "WRITE"} en {t1_stop-t1_start}')
    logging.info("Thread %s: finishing", name)

    # Save info into a csv file
    df = pd.DataFrame([["READ" if query == query_read else "WRITE", datetime.datetime.now(), name, t1_stop - t1_start]],
                      columns=['Operation', 'time', 'id_thread', 'time [s]'])
    df.to_csv('nebula.csv', index=False, mode='a', header=False)

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS_SIZE) as executor:
        executor.map(thread_function, range(THREADS_SIZE))
