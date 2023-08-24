import random
from py2neo import Graph
import time

# Configuración de conexión a Neo4j
graph = Graph("bolt://localhost:7688", user="neo4j", password="fedeymanu", name="neo4j2")

# Función para ejecutar la consulta
def execute_query():
    query = (
        """MATCH (u1:User)-[:RECOMMENDS]->(g:Game)<-[:RECOMMENDS]-(u2:User)
            WHERE u1.user_id <> u2.user_id
            RETURN u1.user_id, u2.user_id, g.title"""
    )
    result = graph.run(query)
    return result

# Ejecutar consulta y medir tiempo
start_time = time.time()
result = execute_query()
end_time = time.time()

# Imprimir resultados
# no imprimir resultados porque son muchos
#print(result.data())
print(f"Tiempo de ejecución: {end_time - start_time:.3f} segundos")
