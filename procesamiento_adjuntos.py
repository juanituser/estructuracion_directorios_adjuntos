import os
import pandas as pd
from PIL import Image
from PyPDF2 import PdfMerger

# Leer el archivo CSV
paths_df = pd.read_csv('C:/Users/PC/Documents/CEICOL/Documentacion/validadores propios/prueba/prueba.csv')
adm_source_categories = ['_DI', '_EP', '_AD', '_SJ', '_DP']
paths_attachment_property_by_category = paths_df[['sufijo', 'predio']].value_counts(sort=True).reset_index(name='conteo')
root_directory = 'C:/Users/PC/Documents/CEICOL/Documentacion/scripts/adjuntos_crudos/'
final_directory = 'C:/Users/PC/Documents/CEICOL/Documentacion/scripts/adjuntos_procesados/'

def attachment_processing(source_dir, target_dir):
    # Recorrer todas las carpetas en source_dir
    for dirpath, dirnames, filenames in os.walk(source_dir):
       
        # Crear la ruta de salida correspondiente
        relative_path = os.path.relpath(dirpath, source_dir)
        output_dir = os.path.join(target_dir, relative_path)
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

    for i in range(paths_attachment_property_by_category.shape[0]):
        category_property_count_row = paths_attachment_property_by_category.iloc[i]
        filtered_paths = paths_df[(paths_df['predio'] == category_property_count_row['predio']) & (paths_df['sufijo'] == category_property_count_row['sufijo']) & (paths_df['sufijo'].isin(adm_source_categories))]
        if not filtered_paths.empty:
            filtered_paths = filtered_paths.sort_values(by='nombre')  # Asegúrate de que 'nombre' exista en tus datos
            pdfs_to_combine = []  
            
            base_path = ''

            for index, row in filtered_paths.iterrows():
                base_path = directorio + row['ruta_base']
                name = row['nombre']
                pdf_file_name = name + '.pdf'
                full_path = os.path.join(base_path, pdf_file_name)
                pdfs_to_combine.append(full_path)
            if len(pdfs_to_combine) > 1:
                merge_pdfs(pdfs_to_combine)
            else:
                print(f"No hay suficientes archivos PDF para fusionar en: {base_path}")

attachment_processing(root_directory, final_directory)          
pdfs_to_merge(final_directory)

    

