# Librerías para gestión de tiempos
from time import sleep
from tqdm import tqdm

# Librerías para tratamiento de datos

import pandas as pd
import pickle
import numpy as np
import re

# Librerías para captura de datos
import requests
from bs4 import BeautifulSoup

# Librería de traducción
from googletrans import Translator

# Librería de geolocalización
from geopy.geocoders import Nominatim

# Librerías para automatización de navegadores web con Selenium
from selenium import webdriver  
from webdriver_manager.chrome import ChromeDriverManager  
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.support.ui import Select 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException 

# Librería para gestión de tiempos
import time

# Librería para trabajar con bases de datos SQL
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Librería para gestionar ficheros del sistema y archivos .env, para cargar tokens y claves
import os
import dotenv
dotenv.load_dotenv()

# Librería para ignorar avisos
import warnings
warnings.filterwarnings("ignore") # Ignora TODOS los avisos


# -------------------------------------- #

# Este script permite navegar y extraer información de un sitio web, limpiar y organizar los datos,
# y luego interactuar con una base de datos para almacenar o recuperar información.

# -------------------------------------- #

# Importamos el usuario y contraseña que hemos guardado en el archivo .env, de modo que podamos utilizarlos como inputs de nuestra función.
dbeaver_pw = os.getenv("dbeaver_pw")
dbeaver_user = os.getenv("dbeaver_user")
rapiapi_key = os.getenv("rapiapi_key")
ruta_descarga = os.getenv("ruta_descarga")

def consulta_peliculas(paginas):
    url = "https://moviesdatabase.p.rapidapi.com/titles"
    headers = {
        "x-rapidapi-key": rapiapi_key,
        "x-rapidapi-host": "moviesdatabase.p.rapidapi.com"
    }
    
    lista_generos = ["Drama", "Comedy", "Action", "Fantasy", "Horror", "Mystery", "Romance", "Thriller"]
    lista_tipos = ["movie", "short"]
    
    lista_completa = []

    for genero in tqdm(lista_generos, desc="Procesando géneros"):
        for tipo in lista_tipos:
            for year in range(1990, 2025):
                for pagina in range(1, paginas + 1):
                    querystring = {
                        "genre": str(genero),
                        "titleType": str(tipo),
                        "year": str(year),
                        "page": str(pagina),
                        "limit": "50"
                    }
                    
                    try:
                        response = requests.get(url, headers=headers, params=querystring)
                        response.raise_for_status()  # Raise an exception for HTTP errors
                        res = response.json()
                        
                        peliculas = res.get("results", [])
                            
                        for pelicula in peliculas:
                            id = pelicula.get("id", None)
                            tipo = pelicula.get("titleType", {}).get("text", None)
                            nombre = pelicula.get("titleText", {}).get("text", None)
                            ano = pelicula.get("releaseYear", {}).get("year", None)
                            mes = pelicula.get("releaseDate", {}).get("month", "ND")
                            
                            lista_completa.append((id, nombre, tipo, ano, mes, genero))

                    except requests.exceptions.RequestException as e:
                        print(f"Error en la página {pagina}: {e}")
                        continue
                
                    sleep(2)
    
    return lista_completa


def scrap_imdb(lista_ids):

    # Configurar el driver y abrir la URL
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--incognito")
    driver = webdriver.Chrome(options=chrome_options)
    
    url_inicio = f"https://www.imdb.com/"
    driver.get(url_inicio)
    
    # Esperar a que aparezca el banner de cookies y aceptar cookies
    time.sleep(3)
    try:
        driver.find_element("css selector", "#__next > div > div > div.sc-jrcTuL.bPmWiM > div > button.icb-btn.sc-bcXHqe.sc-dkrFOg.sc-iBYQkv.dcvrLS.ddtuHe.dRCGjd").click()
        print("Cookies aceptadas")
    except Exception as e:
        print(f"Error al aceptar cookies: {e}")

    lista_sopas = []
    time.sleep(2)
    
    # Iterar sobre las páginas
    for id in tqdm(lista_ids):
        # Navegar a la página de resultados
        pagina_url = f"https://www.imdb.com/title/{id}"
        driver.get(pagina_url)
        #print(f"Scraping página {id}")

        # Obtener el código fuente de la página y analizarlo con BeautifulSoup
        time.sleep(2)

        try:
            source = driver.page_source
            sopa = BeautifulSoup(source, "html.parser")
            #print(f"Sopa creada {id}")
            time.sleep(1)

            calificacion = sopa.select_one('#__next > main > div > section.ipc-page-background.ipc-page-background--base.sc-afa4bed1-0.iMxoKo > section > div:nth-child(5) > section > section > div.sc-9a2a0028-3.bwWOiy > div.sc-3a4309f8-0.jJkxPn.sc-70a366cc-1.kUfGfN > div > div:nth-child(1) > a > span > div > div.sc-d541859f-0.hNIoIx > div.sc-d541859f-2.kxphVf > span.sc-d541859f-1.imUuxf')
            direccion = sopa.select_one('#__next > main > div > section.ipc-page-background.ipc-page-background--base.sc-afa4bed1-0.iMxoKo > section > div:nth-child(5) > section > section > div.sc-9a2a0028-4.eZLbYw > div.sc-9a2a0028-6.iMFgzR > div.sc-9a2a0028-10.dFokEJ > section > div.sc-70a366cc-3.iwmAOx > div > ul > li:nth-child(1) > div')
            guionista = sopa.select_one('#__next > main > div > section.ipc-page-background.ipc-page-background--base.sc-afa4bed1-0.iMxoKo > section > div:nth-child(5) > section > section > div.sc-9a2a0028-4.eZLbYw > div.sc-9a2a0028-6.iMFgzR > div.sc-9a2a0028-10.dFokEJ > section > div.sc-70a366cc-3.iwmAOx > div > ul > li:nth-child(2) > div')
            argumento = sopa.select_one('#__next > main > div > section.ipc-page-background.ipc-page-background--base.sc-afa4bed1-0.iMxoKo > section > div:nth-child(5) > section > section > div.sc-9a2a0028-4.eZLbYw > div.sc-9a2a0028-6.iMFgzR > div.sc-9a2a0028-10.dFokEJ > section > p > span.sc-3ac15c8d-1.gkeSEi')        
            duracion = sopa.select_one('#__next > main > div > section.ipc-page-background.ipc-page-background--base.sc-afa4bed1-0.iMxoKo > section > div:nth-child(5) > section > section > div.sc-9a2a0028-3.bwWOiy > div.sc-70a366cc-0.bxYZmb > ul > li:nth-child(2)')

            calificacion = calificacion.text.strip()
            direccion = direccion.text.strip()
            guionista = guionista.text.strip()
            argumento = argumento.text.strip()
            duracion = duracion.text.strip()

            # Almacenar resultados de la página
            lista_sopas.append((id, calificacion, direccion, guionista, argumento, duracion))

            with open('datos/tuplas_sopas.pkl', 'wb') as f:
                pickle.dump(lista_sopas, f)
        
        except:
            pass
    
    # Cerrar el navegador
    time.sleep(2)
    driver.quit()
    
    # Imprimir confirmación
    print("Scraping finalizado")
    
    return lista_sopas


def dbeaver_crear_db(database_name):
    """
    Crea una base de datos de PostgreSQL si aún no existe.

    Parámetros:
    -----------
    database_name : str
        El nombre de la base de datos a crear.

    Esta función se conecta al servidor PostgreSQL usando credenciales de usuario, verifica si una base 
    de datos con el nombre dado existe y la crea si no existe. Si ocurre un error de conexión, la función 
    imprimirá el tipo específico de error, como una contraseña incorrecta o un problema de conexión.

    Dependencias:
    -------------
    Requiere el paquete psycopg2 y las siguientes variables globales:
    - dbeaver_user: str - El nombre de usuario para conectarse a PostgreSQL.
    - dbeaver_pw: str - La contraseña asociada con el nombre de usuario.

    Retorna:
    --------
    None
    """

    try:
        conexion = psycopg2.connect(
            dbname="postgres",
            user=dbeaver_user,
            password=dbeaver_pw,
            host="localhost",
            port="5432"
        )

        conexion.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Crear un cursor con la nueva conexión
        cursor = conexion.cursor()
        
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
        
        # Almacenar el resultado de fetchone; si existe, tendrá una fila, de lo contrario None
        bbdd_existe = cursor.fetchone()
        
        # Si bbdd_existe es None, crear la base de datos
        if not bbdd_existe:
            cursor.execute(f"CREATE DATABASE {database_name};")
            print(f"Base de datos {database_name} creada con éxito")
        else:
            print("La base de datos ya existe")
            
        # Cerrar el cursor y la conexión
        cursor.close()
        conexion.close()

    except OperationalError as e:
        if e.pgcode == errorcodes.INVALID_PASSWORD:
            print("Contraseña es errónea")
        elif e.pgcode == errorcodes.CONNECTION_EXCEPTION:
            print("Error de conexión")
        else:
            print(f"Ocurrió el error {e}")


def dbeaver_conexion(database):
    """
    Establece una conexión a una base de datos DBeaver.

    Args:
        database (str): El nombre de la base de datos.

    Returns:
        connection: Un objeto de conexión a la base de datos.
    """
    try:
        conexion = psycopg2.connect(
            database=database,
            user=dbeaver_user,
            password=dbeaver_pw,
            host="localhost",
            port="5432"
        )
    except OperationalError as e:
        if e.pgcode == errorcodes.INVALID_PASSWORD:
            print("Contraseña es errónea")
        elif e.pgcode == errorcodes.CONNECTION_EXCEPTION:
            print("Error de conexión")
        else:
            print(f"Ocurrió el error {e}")

    return conexion


def dbeaver_fetch(conexion, query):
    """
    Ejecuta una consulta y obtiene los resultados en un dataframe.

    Args:
        conexion (connection): Un objeto de conexión a la base de datos.
        query (str): La consulta SQL a ejecutar.

    Returns:
        list: Los resultados de la consulta en un dataframe.
    """
    cursor = conexion.cursor()
    cursor.execute(query)
    # resultado_query = cursor.fetchall()
    # Si quisiéramos que el resultado fuera en forma de lista podríamos utilizar esta línea de código.
    # En este caso, sin embargo, nos interesa obtener directamente DFs.
    
    df = pd.DataFrame(cursor.fetchall())
    df.columns = [col[0] for col in cursor.description]

    cursor.close()
    conexion.close()

    return df


def dbeaver_commit(conexion, query, *values):
    """
    Ejecuta una consulta y realiza un commit de los cambios.

    Args:
        conexion (connection): Un objeto de conexión a la base de datos.
        query (str): La consulta SQL a ejecutar.
        *values: Los valores a incluir en la consulta.

    Returns:
        str: Un mensaje de confirmación después del commit.
    """
    cursor = conexion.cursor()
    cursor.execute(query, *values)
    conexion.commit()
    cursor.close()
    conexion.close()
    return print("Commit realizado")


def dbeaver_commitmany(conexion, query, *values):
    """
    Ejecuta múltiples consultas y realiza un commit de los cambios.

    Args:
        conexion (connection): Un objeto de conexión a la base de datos.
        query (str): La consulta SQL a ejecutar.
        *values: Los valores a incluir en la consulta.

    Returns:
        str: Un mensaje de confirmación después del commit.
    """
    cursor = conexion.cursor()
    cursor.executemany(query, *values)
    conexion.commit()
    cursor.close()
    conexion.close()
    return print("Commit realizado")


def csvs_a_tuplas(rutas_archivos):
    """
    Lee múltiples archivos CSV y convierte sus datos en listas de tuplas.

    Parámetros:
    rutas_archivos (list): Lista de rutas de archivos CSV que se leerán.

    Retorna:
    list: Una lista de listas de tuplas, donde cada sublista corresponde a un archivo CSV.
          Cada tupla representa una fila de datos sin el índice.

    Ejemplo:
    rutas_archivos = ["ruta/archivo1.csv", "ruta/archivo2.csv"]
    listas_tuplas = csvs_a_tuplas(rutas_archivos)
    # listas_tuplas será una lista que contiene una lista de tuplas para cada archivo.

    """
    listas_tuplas = []
    for ruta in rutas_archivos:
        df = pd.read_csv(ruta, index_col=0)
        tuplas_df = list(df.itertuples(index=False, name=None))
        listas_tuplas.append(tuplas_df)
    return listas_tuplas


def identificar_outliers(df, columna):
    """
    Identifica outliers en una columna de un DataFrame utilizando el método IQR.
    
    Parámetros:
    df (DataFrame): El DataFrame que contiene la columna a evaluar.
    columna (str): El nombre de la columna a evaluar.

    Retorna:
    DataFrame: Un DataFrame que contiene solo los outliers.
    """
    Q1 = df[columna].quantile(0.25)
    Q3 = df[columna].quantile(0.75)
    IQR = Q3 - Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    outliers = df[(df[columna] < lower_bound) | (df[columna] > upper_bound)]
    return outliers


def estilos_mapa(elemento):
    """
    Función para definir los estilos de los elementos en un mapa de Folium.

    Parámetros:
    elemento (dict): Diccionario con la información del elemento a estilizar.

    Retorna:
    dict: Diccionario con los estilos aplicados al elemento.
    """
    return {
        'fillColor': 'white',  # Relleno
        'color': 'red',      # Borde
        'weight': 2,
        'fillOpacity': 0.6,
        'icon': 'circle',
        'markerColor': 'red', # Marcador
        'prefix': 'fa',
        'iconColor': 'yellow'
    }
