# coding=utf-8
import mysql.connector
import csv, os, bz2, datetime
from mandrill import Mandrill
import json
import requests
from tqdm import tqdm

TODAY=datetime.date.today()
YEAR= TODAY.year
CONFIG_FILE='credentials.json'
CHUNK_SIZE = 1024 * 1024 # 1MB
HEADERS =''
USER_ID = ''
BASE_URL = ''
CSV_FILE = str(YEAR) + '.csv'
COMPRESSED_FILE = str(YEAR) + '.csv.bz2'
starting_day_of_current_year = datetime.date.today().replace(month=1, day=1)

def consultar_bd(query):
    """
    Función para realizar una consulta a la base de datos MySQL y retornar los resultados.

    Args:
        query: La consulta SQL a ejecutar.

    Returns:
        Una lista de tuplas con los resultados de la consulta.
    """


    with open(CONFIG_FILE) as f:        
        data = json.load(f)
        # Extract the necessary parameters from the configuration data
        host = data['db_host']
        user=data['db_user']
        password = data['db_password']
        database = data['db']
    try:
        conexion = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        cursor = conexion.cursor()
        cursor.execute(query)
        resultados = cursor.fetchall()
        total = cursor.rowcount
        cursor.close()
        conexion.close()
        return resultados, total
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def exportar_csv(resultados, total, ruta_archivo):
    """
    Función para exportar los resultados de una consulta a un archivo CSV.

    Args:
        resultados: La lista de tuplas con los resultados de la consulta.
        ruta_archivo: La ruta donde se guardará el archivo CSV.
    """
    
    
    try:
        # Initialize tqdm progress bar
        progress_bar = tqdm(total=total, unit='B', unit_scale=True, desc='Writing CSV')
        with open(ruta_archivo, "w", encoding="utf-8", newline="") as archivo_csv:
            escritor = csv.writer(archivo_csv)
            escritor.writerow(["codigo","razon_social","distribuidor","direccion","latitud","longitud","nom_comuna","nom_region","combustible","precio","unidad_cobro","atencion","fecha_actualizacion","hora_actualizacion","es_electrolinera","es_gasolinera"])
            escritor.writerows(resultados)
        progress_bar.update(total)
    except Exception as e:
        print(f"Error al exportar el archivo CSV: {e}")
    
    progress_bar.close()

def enviar_correo(estado, mensaje = "Mensaje de Correo"):
    """
    Función para enviar un correo electrónico con Mandrill.

    Args:
        estado: El estado de la acción (éxito o error).
        mensaje: El mensaje a enviar en el correo electrónico.
    """
    with open(CONFIG_FILE) as f:        
        data = json.load(f)
        # Extract the necessary parameters from the configuration data
        correo_remitente = data['correo_remitente']
        correos_destinatarios = data['correo_destinatarios']
        api_key_mandrill = data['key_mandrill']

    try:
        mandrill = Mandrill(api_key_mandrill)
        mensaje_correo = {
            "from_email": correo_remitente,
            "to": [{"email": correo} for correo in correos_destinatarios],
            "subject": f"Alerta - {estado}",
            "text": mensaje,
        }
        mandrill.messages.send(message=mensaje_correo)
        print(f"Correo electronico enviado correctamente.")
    except Exception as e:
        print(f"Error al enviar el correo electronico: {e}")

#==[Functions]==
# Function to connect to the OneDrive API and get the access token
def onedrive_connection():
    # Define the path to the configuration file
    onedrive_file = CONFIG_FILE
    
    try:
        # Try to open the configuration file and load the settings into a dictionary
        with open(onedrive_file) as f:        
            data = json.load(f)
        # Extract the necessary parameters from the configuration data
        CLIENT_ID = data['client_id']
        TENANT_ID=data['tenant_id']
        USER_ID = data['user_id']
        CLIENT_SECRET = data['secret']
        BASE_URL= data['base_url']
        # Define the parameters for the request to retrieve an access token 
        payload = {
            'grant_type': 'client_credentials',
            'client_id': CLIENT_ID,
            'client_secret' : CLIENT_SECRET,
            'scope':'https://graph.microsoft.com/.default'
            }

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'    
            }

        endpoint = (f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token')
        # Make a request to retrieve an access token from the OAuth endpoint
        result=requests.get(endpoint, headers=headers, data = payload)
        # Extract the access token from the response and store it in a variable  
        access_token = json.loads(result.text)['access_token']
        # Define the headers to be used for future requests, including the access token
        headers = {
            'Authorization': 'Bearer ' + access_token,
            'Connection': 'Keep-Alive',
            'Content-Length': '0'
            }
        
        # Return the headers, user ID, and base URL to the calling function
        return headers, USER_ID, BASE_URL
    
    # Catch any errors that might occur during the process and print an error message
    except (FileNotFoundError, KeyError, requests.exceptions.RequestException) as e:
        print(f'Error: {e}')
        # Return None values to indicate that the function failed to complete
        return None, None, None

# Function to compress a file using the BZ2 compression algorithm in chunks.
def compress_file_in_chunks(input_file:str, compressed_file:str):

    # Open the input file in binary mode and the output file in write binary mode using a with block
    with open(input_file, 'rb') as f, open(compressed_file, 'wb') as bzfile:

        # Get the total file size
        total_size = os.path.getsize(input_file)
        # Initialize a BZ2 compressor object
        compressor = bz2.BZ2Compressor()
        # Initialize tqdm progress bar
        progress_bar = tqdm(total=total_size, unit='B', unit_scale=True, desc='Compressing')
        # Loop through the file in chunks
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b''):
            # Compress the chunk of data using the BZ2 compressor object
            compressed_data = compressor.compress(chunk)
            # Write the compressed data to the output file
            bzfile.write(compressed_data)
            # Update the progress bar with the length of the chunk that was just read   
            progress_bar.update(len(chunk))
        # Flush any remaining data from the compressor object and write it to the output file
        bzfile.write(compressor.flush())  
        # Close the tqdm progress bar
        progress_bar.close()
    
    # The function returns nothing
    return

# Function to upload a file to Microsoft OneDrive in chunks
def upload_file_in_chunks(compressed_file):
    #https://learn.microsoft.com/en-us/onedrive/developer/rest-api/api/driveitem_put_content?view=odsp-graph-online
    #https://learn.microsoft.com/en-us/graph/api/driveitem-createuploadsession?view=graph-rest-1.0
            # Try to open the configuration file and load the settings into a dictionary
    
    with open(CONFIG_FILE) as f:        
        data = json.load(f)
    # Extract the necessary parameters from the configuration data
    HOME_PATH = data['home_path']
    
    # Get the name of the file
    file_name = os.path.basename(compressed_file)
    # Construct the endpoint URL for creating an upload session on OneDrive
    endpoint = BASE_URL + f'users/{USER_ID}/drive/items/root:' + HOME_PATH + file_name + ':/createUploadSession'
    # Send a POST request to create an upload session on OneDrive
    response_upload_session = requests.post(
        endpoint,
        headers=HEADERS)
    try:
        # Get the upload URL from the response
        upload_url = response_upload_session.json()['uploadUrl']
    except Exception as e:
        # If there's an error getting the upload URL, raise an exception
        raise Exception(str(e))    
    # Get the size of the file in bytes
    file_size = os.path.getsize(file_name)

    # Open the file in binary mode
    with open(file_name, "rb") as f:
        # Initialize the start and end bytes of the chunk
        start_byte = 0
        end_byte = min(CHUNK_SIZE, file_size)

        # Use tqdm to show a progress bar while uploading the file
        with tqdm(total=file_size, unit="B", unit_scale=True,desc='Uploading') as pbar:

            # Loop through the file in chunks and upload each chunk to OneDrive
            while start_byte < file_size:
                # Construct the headers for the chunk upload
                headers = {
                    'Content-Length':f'{CHUNK_SIZE}',
                    'Content-Range': f'bytes {start_byte}-{end_byte-1}/{file_size}'
                    }
                # Read the next chunk from the file
                chunk = f.read(CHUNK_SIZE)
                # Upload the chunk to OneDrive using a PUT request)
                chunk_data_upload_status = requests.put(
                        upload_url,
                        headers=headers,
                        data=chunk)                    
                #print(chunk_data_upload_status.json())
                # Update the progress bar with the length of the chunk that was just uploaded
                pbar.update(len(chunk))
                # Update the start and end bytes for the next chunk
                start_byte = end_byte
                end_byte = min(start_byte + CHUNK_SIZE, file_size)

    # Return nothing when the upload is complete            
    return

# Definir la consulta SQL
query = f"""
SELECT   eess.estacion_id                    AS codigo
        ,razon.razon_social                  AS razon_social
        ,tipos_marcas.nombre                 AS distribuidor
        ,ubicacion.direccion                 AS direccion
        ,ubicacion.latitud                   AS latitud
        ,ubicacion.longitud                  AS longitud
        ,comunas.nom_comuna                  AS comuna
        ,regiones.nom_region                 AS region
        ,tipos_combustibles.nombre_corto     AS combustible
        ,precio_combustible.precio           AS precio
        ,unidades_cobros.nombre_corto        AS unidad_cobro
        ,tipos_atenciones.nombre             AS atencion
        ,date(precio_combustible.updated_at) AS fecha_actualizacion
        ,time(precio_combustible.updated_at) AS hora_actualizacion
        ,tipo_estacion.es_electrolinera
        ,tipo_estacion.es_gasolinera
FROM estaciones_servicios eess
INNER JOIN precio_combustible FOR SYSTEM_TIME BETWEEN '{starting_day_of_current_year}' AND NOW()
ON eess.id = precio_combustible.estacion_id
INNER JOIN estacion_suministra_combustible esc
ON precio_combustible.combustible_id = esc.combustible_id and eess.id = esc.estacion_id
INNER JOIN tipos_combustibles
ON esc.combustible_id = tipos_combustibles.id
INNER JOIN unidades_cobros
ON tipos_combustibles.unidad_cobro = unidades_cobros.id
INNER JOIN tipos_atenciones
ON tipos_atenciones.id = tipos_combustibles.tipo_atencion
INNER JOIN estacion_estado estado
ON eess.id = estado.estacion_id
INNER JOIN estacion_mantencion mtto
ON eess.id = mtto.estacion_id
LEFT JOIN estacion_razon_social razon
ON eess.id = razon.estacion_id
INNER JOIN estacion_direccion ubicacion
ON eess.id = ubicacion.estacion_id
INNER JOIN tipo_estacion tipo_estacion
ON eess.id = tipo_estacion.estacion_id
LEFT JOIN estacion_horario_atencion horario
ON eess.id = horario.id_estacion AND horario.activo = 1
LEFT JOIN tipos_estacion_horario_atencion tipo_horario
ON horario.id_tipo_h_a = tipo_horario.id
INNER JOIN tipos_marcas
ON eess.tipo_marca_id = tipos_marcas.id
INNER JOIN regiones
ON ubicacion.region_id = regiones.cod_region
INNER JOIN comunas
ON ubicacion.comuna_id = comunas.cod_comuna
where eess.activo = 1;
"""

# Consultar la base de datos
resultados, total = consultar_bd(query)

# Si la consulta fue exitosa
if resultados:
    HEADERS, USER_ID, BASE_URL = onedrive_connection()
    if HEADERS:
        # Exportar a CSV
        exportar_csv(resultados, total, CSV_FILE)
        # Compress the CSV file in chunks and write the compressed data to the BytesIO buffer
        compress_file_in_chunks(CSV_FILE,COMPRESSED_FILE)
        # Upload the compressed file in chunks
        upload_file_in_chunks(COMPRESSED_FILE)
        # Enviar correo de éxito
        mensaje = "Se ha generado el Archivo Historico de BEL de manera exitosa."
        enviar_correo("Éxito", mensaje)
    else:
        mensaje = "Ha ocurrido un error al acceder a OneDrive"
        enviar_correo("Error", mensaje)

else:
    # Enviar correo de error
    mensaje = "Ha ocurrido un error al generar el Archivo Historico de BEL."
    enviar_correo("Error", mensaje)