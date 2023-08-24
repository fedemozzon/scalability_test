import sys
import csv
import time
import concurrent.futures
from mgclient import connect

# command arguments
# 1: path to csv
# 2: node type
if len(sys.argv) != 3:
    print("Usage: python3 load_csv_jueguitos_memgraph.py <path_to_csv> <node_type>")
    sys.exit(1)

# Path al archivo CSV
archivo_csv = sys.argv[1]
# Tipo de nodo
tipo_nodo = sys.argv[2]

# Establecer la conexión a la base de datos Memgraph
conn = connect(host="localhost", port=7687)
db = conn.cursor()

# Función para cargar nodos desde un archivo CSV
def cargar_nodo(row, tipo_nodo):
    inicio = time.time()
    query = f"CREATE (:{tipo_nodo} $props)"
    params = {"props": row}
    db.execute(query, params)
    conn.commit()
    fin = time.time()
    tiempo = fin - inicio
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
    inicio = time.time()
    query = (
        f"MATCH (a), (b) "
        f"WHERE a.app_id = '{row['app_id']}' AND b.user_id = '{row['user_id']}' "
        f"CREATE (b)-[:{tipo_relacion} {{review_id: '{row['review_id']}', helpful: '{row['helpful']}', funny: '{row['funny']}', date: '{row['date']}', is_recommended: '{row['is_recommended']}', hours: '{row['hours']}'}}]->(a)"
    )
    db.execute(query)
    conn.commit()
    fin = time.time()
    tiempo = fin - inicio
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
with open(f'tiempos_memgraph_{tipo_nodo}.csv', 'w', newline='') as archivo_csv:
    writer = csv.writer(archivo_csv)
    writer.writerow(['Tiempo Individual'])
    for tiempo in tiempos_individuales:
        writer.writerow([tiempo])
