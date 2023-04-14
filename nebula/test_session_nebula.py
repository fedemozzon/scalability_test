from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import pandas as pd
from typing import Dict
from nebula3.data.ResultSet import ResultSet

import logging
import threading
import time

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
# config = Config()

# # init connection pool
# connection_pool = ConnectionPool()

# # if the given servers are ok, return true, else return false
# ok = connection_pool.init([('127.0.0.1', 9669)], config)

# # option 2 with session_context, session will be released automatically
# with connection_pool.session_context('root', 'root') as session:
#     print("CONNECTING TO NEBULA GRAPH")
#     session.execute('USE space1')
#     print("USE SPACE space1")
#     # Aca poner la query que se nos cante
#     query = "MATCH (n) RETURN n LIMIT 100;"
#     result = session.execute(query)
#     print(f"QUERY {query} EXECUTED")
#     # df = result_to_df(result)
#     # print(df)
#     print("RESULTS -->")
#     print(result)
#     # Capaz aca imprimir el resultado con un for o hacer algo

# # close the pool
# connection_pool.close()

threads_size = 7
def create_thread():
    logging.info("Main    : create and start thread %d.", index)
    x = threading.Thread(target=thread_function, args=(index,))
    threads.append(x)
    x.start()

def thread_function(name):
    logging.info("Thread %s: starting", name)
    time.sleep(2)
    logging.info("Thread %s: finishing", name)

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")
threads = list()
for index in range(threads_size):
    create_thread()
for index, thread in enumerate(threads):
    logging.info("Main    : before joining thread %d.", index)
    thread.join()
    logging.info("Main    : thread %d done", index)