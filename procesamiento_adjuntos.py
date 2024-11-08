import os
import pandas as pd
from PIL import Image
from PyPDF2 import PdfMerger

# Leer el archivo CSV
rutas = pd.read_csv('C:/Users/PC/Documents/CEICOL/Documentacion/validadores propios/prueba/prueba.csv')
categorias_fuente = ['_DI', '_EP', '_AD', '_SJ', '_DP']
rutas_adjuntos_predio_por_categoria = rutas[['sufijo', 'predio']].value_counts(sort=True).reset_index(name='conteo')
valor_directorio_raiz = 'C:/Users/PC/Documents/CEICOL/Documentacion/scripts/adjuntos_crudos/'
valor_directorio_final = 'C:/Users/PC/Documents/CEICOL/Documentacion/scripts/adjuntos_procesados/'

def attachment_processing(directorio_raiz, directorio_final):
    # Recorrer todas las carpetas en directorio_raiz
    for dirpath, dirnames, filenames in os.walk(directorio_raiz):
       
        # Crear la ruta de salida correspondiente
        relative_path = os.path.relpath(dirpath, directorio_raiz)
        output_dir = os.path.join(directorio_final, relative_path)
        os.makedirs(output_dir, exist_ok=True)

        # Procesar cada archivo en la carpeta
        for filename in filenames:
            if filename.lower().endswith(('.jpg', '.jpeg', '.png')):
                image_path = os.path.join(dirpath, filename)
                pdf_name = os.path.splitext(filename)[0] + ".pdf"
                pdf_path = os.path.join(output_dir, pdf_name)

                try:
                    # Convertir la imagen a PDF
                    img = Image.open(image_path).convert("RGB")
                    img.save(pdf_path)
                    print(f"PDF guardado como: {pdf_path}")

                except Exception as e:
                    print(f"Error al procesar {image_path}: {e}")
                    
            else:
                # Copiar archivos que no son imágenes
                source_path = os.path.join(dirpath, filename)
                destination_path = os.path.join(output_dir, filename)
                
                try:
                    # Copiar archivo manualmente
                    with open(source_path, 'rb') as src_file:
                        with open(destination_path, 'wb') as dst_file:
                            dst_file.write(src_file.read())
                    print(f"Archivo copiado a: {destination_path}")
                    
                except Exception as e:
                    print(f"Error al copiar {source_path}: {e}")
 


def merge_pdfs(arreglo_pdfs):
    pdf_merged = arreglo_pdfs[0]
    merger = PdfMerger()
    for pdf in arreglo_pdfs:
        merger.append(pdf)
    merger.write(pdf_merged)
    merger.close()
    print(f"Archivos PDF fusionados en: {pdf_merged}")

    del arreglo_pdfs[0]
    for pdf in arreglo_pdfs:
        os.remove(pdf)
        print(f"Archivo PDF eliminado: {pdf}")
    
def pdfs_to_merge(directorio):

    for i in range(rutas_adjuntos_predio_por_categoria.shape[0]):
        temp_fila = rutas_adjuntos_predio_por_categoria.iloc[i]
        temp_rutas = rutas[(rutas['predio'] == temp_fila['predio']) & (rutas['sufijo'] == temp_fila['sufijo']) & (rutas['sufijo'].isin(categorias_fuente))]
        if not temp_rutas.empty:
            temp_rutas = temp_rutas.sort_values(by='nombre')  # Asegúrate de que 'nombre' exista en tus datos
            pdfs_a_unir = []  
            
            ruta_base = ''

            for index, row in temp_rutas.iterrows():
                ruta_base = directorio + row['ruta_base']
                nombre = row['nombre']
                nombre_archivo = nombre + '.pdf'
                ruta_completa = os.path.join(ruta_base, nombre_archivo)
                pdfs_a_unir.append(ruta_completa)
            if len(pdfs_a_unir) > 1:
                merge_pdfs(pdfs_a_unir)
            else:
                print(f"No hay suficientes archivos PDF para fusionar en: {ruta_base}")

attachment_processing(valor_directorio_raiz, valor_directorio_final)          
pdfs_to_merge(valor_directorio_final)

    

