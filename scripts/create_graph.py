import sys
import pandas as pd
import matplotlib.pyplot as plt

# cargar nombre como primer argumento, si no existe terminar
if len(sys.argv) < 2:
    print("Falta el nombre del archivo CSV")
    exit()

filename = sys.argv[1]

# Cargar el archivo CSV
archivo_csv = filename
dataframe = pd.read_csv(archivo_csv)

# Obtener los valores distintos de la columna "operation"
valores_operacion = dataframe["operation"].unique()

# Crear el gráfico de líneas con datos promediados
plt.figure(figsize=(10, 6))  # Tamaño del gráfico (opcional)

for valor in valores_operacion:
    datos_filtrados = dataframe[dataframe["operation"] == valor]
    datos_promediados = datos_filtrados["time_finish"].rolling(500000, min_periods=1).mean()
    datos_x = range(0, len(datos_filtrados), 500000)
    datos_x_promediados = range(0, len(datos_promediados) * 500000, 500000)  # Corregir generación de valores para el eje X
    plt.plot(datos_x_promediados, datos_promediados, label=valor)

# Configurar etiquetas y título del gráfico
plt.xlabel("Cantidad de filas (cada 500.000 líneas)")
plt.ylabel("tiempo promedio (ms)")
plt.title("Gráfico de nombre_de_la_columna promediado (mediana) cada 500.000 líneas")
plt.legend()  # Agregar leyenda

# Mostrar el gráfico
plt.show()

