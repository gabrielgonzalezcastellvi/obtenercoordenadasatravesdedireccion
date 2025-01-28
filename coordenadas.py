import argparse
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import time
import pandas as pd
import mysql.connector
import multiprocessing
import tempfile
import sys
from datetime import datetime
from selenium.common.exceptions import TimeoutException
from seleniumwire import webdriver
import os
import re
from urllib.parse import unquote

path = 'geckodriver.exe'

# CONEXION BASE DE DATOS
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="portout",
    port=3306
)

cursor = db.cursor()

def presentacion():
    print("\n" + "="*80)
    sys.stdout.write(f"\033[F\033[F\033[F")
    print(" By Gabriel Gonzalez Castellvi.")
    print("="*80 + "\n")

def worker(worker, linea):
    firefox_options = Options()
    firefox_options.binary_location = r"C:\Program Files\Mozilla Firefox\firefox.exe"  
    firefox_options.add_argument('--headless')
    firefox_options.add_argument('--no-sandbox')
    firefox_options.add_argument('--disable-gpu')
    firefox_options.add_argument('--log-level=3')
    firefox_options.add_argument('--disable-extensions')
    firefox_options.add_argument('--disable-infobars')
    firefox_options.add_argument('--disable-dev-shm-usage') 

    service = Service(executable_path=path)
    driver = webdriver.Firefox(service=service, options=firefox_options)

    sys.stderr = open(os.devnull, 'w')

    driver.get('https://www.google.com/maps')

    time.sleep(10)

    while len(linea) > 1:
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH,'//*[@id="searchboxinput"]'))).clear()
            time.sleep(5)
        
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH,'//*[@id="searchboxinput"]'))).send_keys(linea[1].strip())
            time.sleep(5)
            WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="searchbox-searchbutton"]'))).click()
            time.sleep(5)

            url_actual = driver.current_url

            print(url_actual)

            # Verificar si los substrings existen en la URL
            try:
                if ",+" in url_actual:
                    cp_start = url_actual.index(",+") + 2
                    cp_end = url_actual.index("+", cp_start)
                    codigo_postal = url_actual[cp_start:cp_end]

                    locality_province_start = cp_end + 1
                    locality_province_end = url_actual.find("/", locality_province_start)

                    if locality_province_end != -1:
                        localidad_provincia = url_actual[locality_province_start:locality_province_end].replace("+", " ").strip()

                        # Asegurarse de que localidad_provincia tenga al menos una coma para dividir
                        if ", " in localidad_provincia:
                            localidad, provincia = localidad_provincia.split(", ")
                        else:
                            localidad = provincia = localidad_provincia

                        match_coords = re.search(r"3d(-?\d+\.\d+)!4d(-?\d+\.\d+)", url_actual)
                        if match_coords:
                            latitud = match_coords.group(1)
                            longitud = match_coords.group(2)
                        else:
                            latitud = "No encontrado"
                            longitud = "No encontrado"

                        direccion = linea[1].strip()
                        print(f"==========================================================================")
                        print(f"Direccion: {direccion}")
                        print(f"Código Postal: {codigo_postal}")
                        print(f"Localidad: {localidad}")
                        print(f"Provincia: {provincia}")
                        coordenadas = latitud + ',' + longitud
                        print(f"Coordenadas: {coordenadas}")
                        
                        # Insertar en la base de datos
                        insert_query = """
                                    INSERT INTO direcciones (direccion, cp, localidad, provincia, coordenadas, fechafiltrado)
                                    VALUES (%s, %s, %s, %s, %s, NOW())
                                """
                        cursor.execute(insert_query, (direccion, codigo_postal, localidad, provincia, coordenadas))
                        db.commit()
                        time.sleep(3)               

                linea.pop(1)
                time.sleep(2)

            except Exception as e:
                print(f"Error durante la ejecución del bucle: {e}")
                linea.pop(1)

        except Exception as e:
            print(f"Error de ejecución: {e}")
            break

    print("Proceso completado.")
    driver.quit()
    cursor.close()
    db.close()

if __name__ == "__main__":
    presentacion()

    file = open('pruebas.csv', 'r', encoding='utf-8', errors='ignore')
    linea = file.readlines()
    file.close()

    instancias = int(input("Instancias: "))

    def splits_list(alist, wanted_parts=1):
        length = len(alist)
        return [alist[i * length // wanted_parts:(i + 1) * length // wanted_parts] for i in range(wanted_parts)]
    
    jobs = []
    new_linea = splits_list(linea, wanted_parts=instancias)
    for i in range(instancias):
        jobs.append(multiprocessing.Process(target=worker, args=(i, new_linea[i])))
    
    for job in jobs:
        job.start()
    
    for job in jobs:
        job.join()
