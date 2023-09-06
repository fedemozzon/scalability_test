import sys
import pandas as pd
import matplotlib.pyplot as plt

# Cargar nombre del archivo CSV como primer argumento
if len(sys.argv) < 4:
    print("Falta el nombre del archivo CSV, el intervalo o el titulo del nodo / relacion")
    sys.exit(1)

filename = sys.argv[1]
intervalo = int(sys.argv[2])
titulo = sys.argv[3]

# Cargar el archivo CSV
archivo_csv = filename
dataframe = pd.read_csv(archivo_csv)

# Obtener los valores de la columna "Tiempos Individuales"
valores_tiempo = dataframe["Tiempo Individual"].tolist()

# Crear el gráfico de evolución de tiempo con promedio cada 1000 filas
plt.figure(figsize=(10, 6))  # Tamaño del gráfico (opcional)

# Configurar etiquetas y título del gráfico
plt.xlabel(f"Número de filas (cada {intervalo})")
plt.ylabel("Tiempo Promedio")
plt.title(f"Evolución del Tiempo con Promedio cada {intervalo} filas de {titulo}")

# Calcular el promedio cada 1000 filas
promedios = []
sobrante = len(valores_tiempo) % intervalo

for i in range(0, len(valores_tiempo) - sobrante, intervalo):  # Ignorar las últimas 70 filas
    promedio = sum(valores_tiempo[i:i+intervalo]) / intervalo
    promedios.append(promedio)

# Añadir el promedio de las últimas 70 filas
promedios.append(sum(valores_tiempo[-sobrante:]) / sobrante)

# Crear una lista de índices correspondientes al promedio cada 1000 filas
indices = [i * intervalo for i in range(len(promedios))]

# Dibujar los datos en el gráfico
plt.plot(indices, promedios)

# Mostrar el gráfico
plt.show()
