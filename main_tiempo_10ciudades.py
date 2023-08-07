import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base
import psycopg2
from sqlalchemy.ext.declarative import declarative_base

if __name__ == '__main__':
    #cityList = ["London", "New York", "Cordoba", "Taipei", "Buenos Aires", "Mexico DF", "Dublin", "Tilfis", "Bogota", "Tokio"]
    cityList = ["London", "Tokio"] #lista de prueba
    # Creo una lista para almacenar todos los datos de las ciudades
    data = []

    for city in cityList:
        url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid=cc3d07bf7909e147cd7443c0415b0f76&units=metric'
        print(url)
        response = requests.get(url)

        if response.status_code == 200:
            response_json = response.json()
            city_data = {
                "City": city,
                #"Tiempo": response_json['weather']['main'],
                "Descripción": response_json['weather'][0]['description'],                
                "Max Temperature (°C)": response_json['main']['temp_max'],
                "Min Temperature (°C)": response_json['main']['temp_min'],
                "Pressure (hPa)": response_json['main']['pressure'],
                "Humidity (%)": response_json['main']['humidity'],
                #"Last Update": datetime.utcfromtimestamp(response_json['dt']).strftime('%Y-%m-%d %H:%M:%S')
            }
            data.append(city_data)

    # Obtener la fecha actual en el formato deseado
    today = datetime.now().strftime('%Y-%m-%d')

    # Crear el DataFrame a partir de la lista de datos
    df = pd.DataFrame(data)

    # Construir el nombre del archivo CSV
    file_name = f"{today}_weather_data.csv"

    # Guardar el DataFrame en el archivo CSV con el nombre generado
    df.to_csv(file_name, index=False)

    print(df)
#------------------------------------------------------------------------------------------------------------
#CREA BBDD Y TABLA
#------------------------------------------------------------------------------------------------------------
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from psycopg2 import sql

# Establecer la conexión al servidor PostgreSQL
conn = psycopg2.connect(
    database="postgres", user='postgres', password='Joselina1979', host='127.0.0.1', port='5432'
)
conn.autocommit = True

# Crear un cursor
cursor = conn.cursor()

# Verificar si la base de datos "clima" ya existe
check_db_query = "SELECT datname FROM pg_catalog.pg_database WHERE datname='clima';"
cursor.execute(check_db_query)
existing_databases = cursor.fetchall()

if ('clima',) not in existing_databases:
    # Preparar la consulta para crear la base de datos
    create_db_sql = sql.SQL("CREATE DATABASE {}").format(sql.Identifier('clima'))
    
    # Crear la base de datos
    cursor.execute(create_db_sql)
    print("Database 'clima' created successfully.")

# Cerrar el cursor y la conexión a la base de datos del servidor
cursor.close()
conn.close()

# Establecer la conexión a la nueva base de datos 'clima'
conn = psycopg2.connect(
    database="clima", user='postgres', password='Joselina1979', host='127.0.0.1', port='5432'
)
conn.autocommit = True

# Crear un cursor para la nueva conexión
cursor = conn.cursor()

# Verificar si la tabla "datos_climaticos" ya existe
check_table_query = '''
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = 'datos_climaticos'
    )
'''
cursor.execute(check_table_query)
table_exists = cursor.fetchone()[0]

if not table_exists:
    # Consulta SQL para crear la tabla
    create_table_query = '''
        CREATE TABLE datos_climaticos (
            id SERIAL PRIMARY KEY,
            ciudad VARCHAR(100),
            temperatura FLOAT,
            humedad FLOAT,
            fecha TIMESTAMP
        )
    '''
    
    # Ejecutar la consulta para crear la tabla
    cursor.execute(create_table_query)
    print("Table 'datos_climaticos' created successfully.")

# Cerrar el cursor y la conexión
cursor.close()
conn.close()

#------------------------------------------------------------------------------------------------------------
#CONECTAR LA BBDD, LA TABLA E INSERTAR LOS DATOS DEL CSV
#------------------------------------------------------------------------------------------------------------

# Leer el archivo CSV usando Pandas
csv_file_path = f'C:/Users/Klusacek/Documents/API_TIEMPO-master/API_TIEMPO/{file_name}'
df = pd.read_csv(csv_file_path)

# Establecer la conexión a la base de datos
conn = psycopg2.connect(
    database="clima", user='postgres', password='Joselina1979', host='127.0.0.1', port='5432'
)
conn.autocommit = True

# Crear un motor SQLAlchemy
engine = create_engine('postgresql://postgres:Joselina1979@127.0.0.1:5432/clima')

# Cargar los datos del DataFrame en la tabla de la base de datos
table_name = 'datos_climaticos'
df.to_sql(table_name, engine, if_exists='replace', index=False)

# Cerrar la conexión
conn.close()

print(f"Datos cargados exitosamente en la tabla '{table_name}'.")