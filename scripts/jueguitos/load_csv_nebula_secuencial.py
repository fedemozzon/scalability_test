import sys
import csv
import time
from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool

# Command arguments
# 1: path to csv
# 2: node type
if len(sys.argv) != 3:
    print("Usage: python3 load_csv_jueguitos_nebula3.py <path_to_csv> <node_type>")
    sys.exit(1)

# Path al archivo CSV
archivo_csv = sys.argv[1]
# Tipo de nodo
tipo_nodo = sys.argv[2]

# Configuración de conexión a Nebula Graph
config = Config()
config.max_connection_pool_size = 10
config.timeout = 600  # Tiempo de espera en segundos

# Reemplaza con la dirección y el puerto de tu instancia de Nebula Graph
address = ("127.0.0.1", 9669)

pool = ConnectionPool()
pool.init([address], config)
session = pool.get_session('root', 'nebula')

# select space
session.execute("USE space1")

# Función para cargar nodos desde un archivo CSV
def cargar_nodos_desde_csv(archivo_csv, tipo_nodo):
    tiempos_individuales = []
    contador = 1
    
    #pool.init([('127.0.0.1', 9669)], config)
    #client = GraphSpacePool(pool)
    
    try:
        #client.connect('jueguitos', 'root', 'nebula')
        with open(archivo_csv, 'r', encoding='utf-8') as archivo:
            reader = csv.DictReader(archivo)
            for row in reader:
                inicio = time.time()
                query = f"INSERT VERTEX {tipo_nodo}({', '.join(row.keys())}) VALUES {contador}:{tuple(row.values())}"
                session.execute(query)
                fin = time.time()
                tiempo = fin - inicio
                tiempos_individuales.append(tiempo)
                print(f"Se creó el nodo {tipo_nodo} {contador} en {tiempo:.3f} segundos")
                contador += 1
    
    finally:
        pass
     #   session.release()
    
    return tiempos_individuales

# Función para crear relaciones desde un archivo CSV
def crear_relaciones_desde_csv(archivo_csv, tipo_relacion):
    # ESTA PARTE ESTA MALLLL
    tiempos_individuales = []
    contador = 0
    
    #pool = ConnectionPool()
    #pool.init([('127.0.0.1', 9669)], config)
    #client = GraphSpacePool(pool)
    
    try:
        #client.connect('jueguitos', 'root', 'nebula')
        with open(archivo_csv, 'r', encoding='utf-8') as archivo:
            reader = csv.DictReader(archivo)
            
            for row in reader:
                inicio = time.time()
                # Relación con datos: review_id, helpful, funny, date, is_recommend, hours
                result = session.execute("MATCH (n:User) RETURN id(n) LIMIT 1;")
                breakpoint()
                return
                query = (
                    f"INSERT EDGE {tipo_relacion}({', '.join(row.keys())}) VALUES {tuple(row.values())}"
                )
                print(query)
                session.execute(query)
                fin = time.time()
                tiempo = fin - inicio
                tiempos_individuales.append(tiempo)
                print(f"Se creó la relación {contador} en {tiempo:.3f} segundos")
                contador += 1
    
    finally:
        pass
        #client.release()
    
    return tiempos_individuales

# Medir el tiempo total de ejecución del script
tiempo_inicio_total = time.time()

# Cargar nodos desde archivos CSV y medir tiempos
if tipo_nodo == 'Game' or tipo_nodo == 'user':
    tiempos_individuales = cargar_nodos_desde_csv(archivo_csv, tipo_nodo)
else:
    tiempos_individuales = crear_relaciones_desde_csv(archivo_csv, 'RECOMMENDS')

# Calcular el tiempo total de ejecución del script
tiempo_total_total = time.time() - tiempo_inicio_total

# Imprimir tiempo total de ejecución en la consola
print(f"Tiempo total de ejecución del script: {tiempo_total_total:.3f} segundos")

# Guardar tiempos individuales en un archivo CSV
with open(f'tiempos_nebula3_{tipo_nodo}.csv', 'w', newline='') as archivo_csv:
    writer = csv.writer(archivo_csv)
    writer.writerow(['Tiempo Individual'])
    for tiempo in tiempos_individuales:
        writer.writerow([tiempo])
