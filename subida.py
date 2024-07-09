import os
import time
import csv
import mariadb
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuración
RUTA_MONITOREO = "/home/eduardo/Documentos/ServicioSocial/ArchivosGenerados"
EXTENSION_ARCHIVO = ".csv"
INTERVALO_VERIFICACION = 10  # segundos

# Configuración de la base de datos
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Solet123!",
    "database": "eventos"
}

def conectar_bd():
    return mariadb.connect(**DB_CONFIG)

def insertar_datos(conexion, datos):
    cursor = conexion.cursor()
    sql = """INSERT INTO evento 
             (fecha, hora, latitud, longitud, profundidad, magnitud, epicentro, fechaUTC, horaUTC) 
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    cursor.execute(sql, datos)
    conexion.commit()

def procesar_archivo(ruta_archivo):
    with open(ruta_archivo, 'r') as archivo:
        lector_csv = csv.reader(archivo)
        todas_las_filas = list(lector_csv)
        
        if len(todas_las_filas) < 2:
            print(f"El archivo {ruta_archivo} no contiene suficientes datos.")
            return
        
        # Ignorar la primera línea si solo contiene una hora
        if len(todas_las_filas[0]) == 1 and todas_las_filas[0][0].count(':') == 2:
            todas_las_filas = todas_las_filas[1:]
        
        for fila in todas_las_filas:
            if len(fila) < 9:
                print(f"Fila ignorada por formato incorrecto: {fila}")
                continue
            
            try:
                # Combinar los campos del epicentro si fueron separados por la coma
                if len(fila) > 9:
                    epicentro = ', '.join(fila[6:-2])
                    fila = fila[:6] + [epicentro] + fila[-2:]

                datos_procesados = [
                    fila[0],  # fecha (string)
                    fila[1],  # hora (string)
                    float(fila[2]),  # latitud
                    float(fila[3]),  # longitud
                    float(fila[4]),  # profundidad
                    float(fila[5]),  # magnitud
                    fila[6],  # epicentro (string)
                    fila[7],  # fechaUTC (string)
                    fila[8]   # horaUTC (string)
                ]
                
                conexion = conectar_bd()
                try:
                    insertar_datos(conexion, datos_procesados)
                    print(f"Datos insertados correctamente: {fila[0]} {fila[1]}")
                except mariadb.Error as error:
                    print(f"Error al insertar datos: {error}")
                    print(f"Fila que causó el error: {fila}")
                finally:
                    conexion.close()
            
            except (IndexError, ValueError) as e:
                print(f"Error al procesar la fila: {e}")
                print(f"Fila que causó el error: {fila}")

    print(f"Procesamiento del archivo {ruta_archivo} completado.")

class ManejadorEventos(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(EXTENSION_ARCHIVO):
            print(f"Nuevo archivo detectado: {event.src_path}")
            time.sleep(1)  # Esperar 1 segundo para asegurarse de que el archivo se ha escrito completamente
            procesar_archivo(event.src_path)

def monitorear_directorio():
    event_handler = ManejadorEventos()
    observer = Observer()
    observer.schedule(event_handler, RUTA_MONITOREO, recursive=False)
    observer.start()
    print(f"Monitoreando la carpeta: {RUTA_MONITOREO}")
    
    try:
        while True:
            time.sleep(INTERVALO_VERIFICACION)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    monitorear_directorio()
