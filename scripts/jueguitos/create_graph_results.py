import sys
import pandas as pd
import matplotlib.pyplot as plt

# Check for the correct number of command-line arguments
if len(sys.argv) < 5:
    print("Uso: python create_graph_results.py <filename1> <filename2> <filename3> <intervalo filas> <titulo nodo> <db>")
    sys.exit(1)

filenames = sys.argv[1:4]
interval = int(sys.argv[4])
node_title = sys.argv[5]
db = sys.argv[6]

# Crear figura
plt.figure(figsize=(12, 6))

# Recorrer archivos
for i, filename in enumerate(filenames):
    dataframe = pd.read_csv(filename)
    
    # Extraer datos de columna "Tiempo Individual"
    valores_tiempo = dataframe["Tiempo Individual"].tolist()
    
    # Calcular el promedio del intervalo
    moving_avg = []
    for j in range(0, len(valores_tiempo) - interval + 1, interval):
        avg = sum(valores_tiempo[j:j+interval]) / interval
        moving_avg.append(avg)

    # Graficar los datos
    plt.plot(range(0, len(moving_avg) * interval, interval), moving_avg, label=filename.split("_")[1])

# Configurar titulos y labels
plt.xlabel(f"Número de filas (cada {interval})")
plt.ylabel("Tiempo Promedio")
plt.title(f"{db}: Evolución del Tiempo con Promedio cada {interval} filas de {node_title}")
plt.legend()

plt.show()
