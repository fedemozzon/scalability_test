import sys
from py2neo import Graph, Node, Relationship
import csv
import time
import concurrent.futures

# command arguments
# 1: path to csv
# 2: node type
if len(sys.argv) != 3:
    print("Usage: python3 load_csv_jueguitos_neo4j.py <path_to_csv> <node_type>")
    sys.exit(1)

# Path al archivo CSV
archivo_csv = sys.argv[1]
# Tipo de nodo
tipo_nodo = sys.argv[2]

# Establecer la conexión a la base de datos Neo4j
graph = Graph("bolt://localhost:7688", user="neo4j", password="fedeymanu", name="neo4j2")

# Función para cargar nodos desde un archivo CSV
def cargar_nodo(row, tipo_nodo):
    nodo = Node(tipo_nodo, **row)
    inicio = time.time()
    graph.create(nodo)
    fin = time.time()
    tiempo = fin - inicio
    #print(f"Se creó el nodo {nodo} en {tiempo:.3f} segundos")
    return tiempo

def cargar_nodos_desde_csv(archivo_csv, tipo_nodo):
    tiempos_individuales = []
    print("procesando...")
    with open(archivo_csv, 'r', encoding='utf-8') as archivo:
        reader = csv.DictReader(archivo)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for tiempo in executor.map(lambda row: cargar_nodo(row, tipo_nodo), reader):
                tiempos_individuales.append(tiempo)
    return tiempos_individuales

# Función para crear relaciones desde un archivo CSV
def crear_relacion(row, tipo_relacion, contador):
    query = (
        f"MATCH (a), (b) "
        f"WHERE a.app_id = '{row['app_id']}' AND b.user_id = '{row['user_id']}' "
        f"CREATE (b)-[:{tipo_relacion} {{review_id: '{row['review_id']}', helpful: '{row['helpful']}', funny: '{row['funny']}', date: '{row['date']}', is_recommended: '{row['is_recommended']}', hours: '{row['hours']}'}}]->(a)"
    )
    inicio = time.time()
    graph.run(query)
    fin = time.time()
    tiempo = fin - inicio
    print(f"Se creó la relación {contador} en {tiempo:.3f} segundos")
    return tiempo

def crear_relaciones_desde_csv(archivo_csv, tipo_relacion):
    tiempos_individuales = []
    print("procesando...")
    with open(archivo_csv, 'r', encoding='utf-8') as archivo:
        reader = csv.DictReader(archivo)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            contador = 0
            for tiempo in executor.map(lambda row: crear_relacion(row, tipo_relacion, contador), reader):
                tiempos_individuales.append(tiempo)
                contador += 1
    return tiempos_individuales

# Medir el tiempo total de ejecución del script
tiempo_inicio_total = time.time()

# Cargar nodos desde archivos CSV y medir tiempos
if tipo_nodo == 'Game' or tipo_nodo == 'User':
    tiempos_individuales = cargar_nodos_desde_csv(archivo_csv, tipo_nodo)
else:
    tiempos_individuales = crear_relaciones_desde_csv(archivo_csv, 'RECOMMENDS')

# Calcular el tiempo total de ejecución del script
tiempo_total_total = time.time() - tiempo_inicio_total

# Imprimir tiempo total de ejecución en la consola
print(f"Tiempo total de ejecución del script: {tiempo_total_total:.3f} segundos")

# Guardar tiempos individuales en un archivo CSV
with open(f'tiempos_neo4j_{tipo_nodo}.csv', 'w', newline='') as archivo_csv:
    writer = csv.writer(archivo_csv)
    writer.writerow(['Tiempo Individual'])
    for tiempo in tiempos_individuales:
        writer.writerow([tiempo])
