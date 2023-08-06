import sys
from py2neo import Graph, Node, Relationship
import csv
import time

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
def cargar_nodos_desde_csv(archivo_csv, tipo_nodo):
    tiempos_individuales = []
    contador = 0
    with open(archivo_csv, 'r', encoding='utf-8') as archivo:
        reader = csv.DictReader(archivo)
        for row in reader:
            inicio = time.time()
            nodo = Node(tipo_nodo, **row)
            graph.create(nodo)
            fin = time.time()
            tiempo = fin - inicio
            tiempos_individuales.append(tiempo)
            print(f"Se creó el nodo {nodo} en {tiempo} segundos")
            contador += 1
    tiempo_total = sum(tiempos_individuales)
    tiempo_promedio = tiempo_total / len(tiempos_individuales)
    return tiempo_total, tiempo_promedio, tiempos_individuales

# Función para crear relaciones desde un archivo CSV
def crear_relaciones_desde_csv(archivo_csv, tipo_relacion):
    tiempos_individuales = []
    contador = 0
    with open(archivo_csv, 'r', encoding='utf-8') as archivo:
        reader = csv.DictReader(archivo)
        for row in reader:
            inicio = time.time()
            # relacion con datos: review_id, helpful, funny, date, is_recommend, hours
            query = (
                f"MATCH (a), (b) "
                f"WHERE a.app_id = '{row['app_id']}' AND b.user_id = '{row['user_id']}' "
                f"CREATE (b)-[:{tipo_relacion} {{review_id: '{row['review_id']}', helpful: '{row['helpful']}', funny: '{row['funny']}', date: '{row['date']}', is_recommended: '{row['is_recommended']}', hours: '{row['hours']}'}}]->(a)"
            )
            graph.run(query)
            fin = time.time()
            tiempo = fin - inicio
            tiempos_individuales.append(tiempo)
            print(f"Se creó la relación {contador} en {tiempo} segundos")
            contador += 1
    tiempo_total = sum(tiempos_individuales)
    tiempo_promedio = tiempo_total / len(tiempos_individuales)
    return tiempo_total, tiempo_promedio, tiempos_individuales

# Cargar nodos desde archivos CSV y medir tiempos
tiempo_total, tiempo_promedio, tiempos_individuales = cargar_nodos_desde_csv(archivo_csv, tipo_nodo) if tipo_nodo == 'Game' or tipo_nodo == 'User' else crear_relaciones_desde_csv(archivo_csv, 'RECOMMENDS')

# Imprimir tiempos totales y promedios en la consola
print("Tiempos totales y promedios:")
print(f"Total: {tiempo_total:.2f} segundos, Promedio: {tiempo_promedio} segundos")

# Guardar tiempos individuales en un archivo CSV
with open(f'tiempos_neo4j_{tipo_nodo}.csv', 'w', newline='') as archivo_csv:
    writer = csv.writer(archivo_csv)
    writer.writerow(['Tiempo Individual'])
    for tiempo in tiempos_individuales:
        writer.writerow([tiempo])
