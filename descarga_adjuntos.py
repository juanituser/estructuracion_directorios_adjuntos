import os
import requests
import pandas as pd

# Ruta del archivo CSV generado a partir de la base de datos

csv_path = 'C:/Users/PC/Documents/CEICOL/insumos_consolidados/leiva_zona_2_4/adjuntos/rutas.csv'  

# Ruta donde se guardarán los archivos
save_path = 'C:/Users/PC/Documents/CEICOL/insumos_consolidados/leiva_zona_2_4/adjuntos/crudos/'

# Cargar el archivo CSV
df = pd.read_csv(csv_path)  

for index, row in df.iterrows():
    base_path = save_path + row['ruta_base'] 
    file_name = row['nombre']
    file_url = row['url_archivo']
    file_extension = '.' + file_url .split('.')[-1]
    full_file_name = file_name + file_extension 
   
  #  Crea las carpetas si no existen
    os.makedirs(base_path , exist_ok=True)
    
    # Definir la ruta completa en donde se guardará cada adjunto
    full_save_path = (base_path  + '/' + full_file_name)
    
    # Descarga de adjuntos
    try:
        response = requests.get(file_url)
        response.raise_for_status() # Verifica si hubo errores en la descarga
        
        # Guardar el archivo en la ruta especificada
        with open(full_save_path, 'wb') as file:
            file.write(response.content)
        
        print(f"Archivo descargado y guardado en: {full_save_path}")
    
    except requests.exceptions.RequestException as e:
        print(f"Error al descargar {file_url}: {e}")