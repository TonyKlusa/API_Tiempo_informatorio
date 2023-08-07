import requests
import json
from datetime import datetime, timedelta
import pandas as pd
from pandas import json_normalize

#Con este script comenzamos el desarrollo para llegar a la api:
#lat=31&lon=64
API_KEY ="12ce9232f12e69e685acd8b7a27e5b2f"
BASE_URL="https://api.openweathermap.org/data/2.5/weather?"
BASE_URL_5DIAS="https://api.openweathermap.org/data/2.5/forecast?"
coordenada="lat=31&lon=64"
coordList = ["lat=31&lon=64", "lat=40&lon=-73", "lat=-31&lon=-64",
             "lat=25&lon=64", "lat=-34&lon=-58", "lat=19&lon=-99",
             "lat=53&lon=6", "lat=41&lon=44", "lat=4&lon=74",
             "lat=35&lon=139"]


if __name__ == '__main__':

    #Armo el endpoint con la coordenada y la API key
    #url = BASE_URL_5DIAS + f"{coordenada}&appid={API_KEY}"
    url = BASE_URL + f"{coordenada}&appid={API_KEY}&units=metric"
    
    response = requests.get(url)
    
#Revisa si la conexión fue exitosa.
if response.status_code == 200:
    response_json = response.json()

 
# Crear un diccionario con los datos que deseas guardar en el DataFrame
    data = {
            'Ciudad': response_json['name'],
            'Temperatura (°C)': response_json['main']['temp'],
            'Sensación térmica(°C)':response_json['main']['feels_like'],
            'Descripción del clima': response_json['weather'][0]['description'],
            'Humedad (%)': response_json['main']['humidity']
        }
# Crea el DataFrame a partir del diccionario
    df = pd.DataFrame([data])

# Obtiene la fecha actual en el formato que quiero
    today = datetime.now().strftime('%Y-%m-%d')

# Nombre de la ubicación
    location_name = response_json['name']

# Construir el nombre del archivo CSV
    file_name = f"{today}_{location_name}_weather_data.csv"

# Guardar el DataFrame en el archivo CSV con el nombre generado
    df.to_csv(file_name, index=False)

    print(df)
    df

if response.status_code == 401:
  print("Eror 401")

