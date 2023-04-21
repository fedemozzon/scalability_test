import random
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
from neo4j import GraphDatabase
import pandas as pd
from typing import Dict
from nebula3.data.ResultSet import ResultSet

import logging
import time
from random import randint
from time import perf_counter
import concurrent.futures

# Constantes
URL = "bolt://localhost:7687" # Fijarse el puerto de cada nodo mapeado
DB_NAME = "neo4j2"
USER = "neo4j"
PASSWORD = "fedeymanu"

class DatabaseConnection():
    def __init__(self, uri, user, password, database) -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user, password), database=database)

    def close(self):
        self.driver.close()

    @staticmethod
    def _write(txn, name):
        query = ("CREATE (n:Person {name: $name, id: randomUUID()}) "
                 "RETURN n.id AS node_id")
        result = txn.run(query, name=name)
        # Result.single() retorna un objeto neo4j.Record
        record = result.single()
        # Se le puede acceder a los campos del record como si fuera un diccionario
        return record["node_id"]

    @staticmethod
    def _read(txn):
        query = ("MATCH (n) RETURN n LIMIT 100")
        # https://neo4j.com/docs/api/python-driver/current/api.html#result
        # txn.run retorna objeto neo4j.Result
        result = txn.run(query)
        # result.data() retorna los resultados como una lista de diccionarios
        result_list = result.data()
        return result_list

    def read(self):
        with self.driver.session() as session:
            return session.execute_read(self._read)

    def insert(self, name):
        with self.driver.session() as session:
            created_id = session.execute_write(self._write, name)
            print("created node with id " + str(created_id))

THREADS_SIZE = 7

def thread_function(name):
    t1_start = perf_counter()
    logging.info("Thread %s: starting", name)
    query_read = "MATCH (n) RETURN n LIMIT 100;"
    query_write = f'INSERT VERTEX users(name, email) VALUES {randint(10000, 500000)}:("John", "john@example.com");'
    query = random.choice([query_read, query_write])
    t1_stop = perf_counter()
    logging.info(f'El thread {name} ejecutó la query de {"READ" if query == query_read else "WRITE"} en {t1_stop-t1_start}')
    logging.info("Thread %s: finishing", name)

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

# with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS_SIZE) as executor:
#         executor.map(thread_function, range(THREADS_SIZE))

# Conexion a la db
db = DatabaseConnection(URL, USER, PASSWORD, DB_NAME)

# Insert para crear cosas (podemos cambiar la query)
# db.insert("hola")

# Por ahora el read retorna match 100 nodos cualquiera
print(db.read())