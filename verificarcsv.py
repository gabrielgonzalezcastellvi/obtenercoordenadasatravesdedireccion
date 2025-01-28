import pandas as pd
import mysql.connector
from mysql.connector import Error

# Conexión a la base de datos MySQL
try:
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="portout"
    )
    cursor = db.cursor()
    print("Conexión a la base de datos establecida.")
except Error as e:
    print(f"Error de conexión a la base de datos: {e}")
    exit()

# Leer el archivo CSV
csv_file = "pruebas.csv"
try:
    df = pd.read_csv(csv_file, dtype={0: str}, error_bad_lines=False, warn_bad_lines=True)
    print(f"Archivo CSV '{csv_file}' leído correctamente.")
except FileNotFoundError:
    print(f"Error: El archivo '{csv_file}' no se encontró.")
    exit()
except pd.errors.EmptyDataError:
    print(f"Error: El archivo '{csv_file}' está vacío.")
    exit()

# Obtener la lista de líneas desde el archivo CSV
lineas_csv = df.iloc[:, 0].astype(str).tolist()

# Dividir en bloques si la lista es muy grande
bloque_size = 1000
lineas_filtradas = []

for i in range(0, len(lineas_csv), bloque_size):
    bloque = lineas_csv[i:i+bloque_size]

    # Construir la consulta para verificar en las tablas portin1 y portout1
    placeholders = ",".join(["%s"] * len(bloque))
    query = f"""
        SELECT direccion FROM direcciones WHERE direccion IN ({placeholders})
    """
    try:
        cursor.execute(query, bloque)  # Duplicar la lista para cubrir ambos SELECT
        lineas_db = {row[0] for row in cursor.fetchall()}
    except Error as e:
        print(f"Error ejecutando la consulta SQL: {e}")
        db.close()
        exit()

    # Filtrar las líneas que NO están en la base de datos
    lineas_filtradas.extend([linea for linea in bloque if linea not in lineas_db])

# Crear un nuevo DataFrame con las líneas filtradas
df_filtrado = pd.DataFrame(lineas_filtradas, columns=["linea"])

# Guardar el nuevo archivo CSV
output_file = "pruebasfiltrado.csv"
df_filtrado.to_csv(output_file, index=False)
print(f"Archivo filtrado generado: {output_file}")

# Cerrar la conexión a la base de datos
cursor.close()
db.close()
