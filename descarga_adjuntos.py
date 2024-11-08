import os
import requests
import pandas as pd

# Ruta del archivo CSV generado a partir de la base de datos
df = pd.read_csv('C:/Users/PC/Documents/CEICOL/Documentacion/validadores propios/prueba/prueba.csv')  

# Ruta donde se guardarán los archivos
ruta_de_guardado = 'C:/Users/PC/Documents/CEICOL/Documentacion/scripts/adjuntos_crudos/'

for index, row in df.iterrows():
    ruta_base = ruta_de_guardado + row['ruta_base'] 
    nombre = row['nombre']
    url_archivo = row['url_archivo']
    extention = '.' + url_archivo.split('.')[-1]
    nombre_archivo = nombre + extention
   
  #  Crea las carpetas si no existen
    os.makedirs(ruta_base, exist_ok=True)
    
    # Definir la ruta completa en donde se guardará cada adjunto
    ruta_completa = (ruta_base + '/' + nombre_archivo)
    
    # Descarga de adjuntos
    try:
        response = requests.get(url_archivo)
        response.raise_for_status() # Verifica si hubo errores en la descarga
        
        # Guardar el archivo en la ruta especificada
        with open(ruta_completa, 'wb') as file:
            file.write(response.content)
        
        print(f"Archivo descargado y guardado en: {ruta_completa}")
    
    except requests.exceptions.RequestException as e:
        print(f"Error al descargar {url_archivo}: {e}")