import psycopg2 as pg
import os
from dotenv import load_dotenv


def conexion_base_de_datos():

    # Cargamos las variables de entorno del archivo .env
    load_dotenv()

    # Cargamos los parametros de conexion y credenciales de la base de datos
    connection = {
        'host':os.getenv('POSTGRES_HOST'),
        'port':os.getenv('POSTGRES_PORT'),
        'user':os.getenv('POSTGRES_USER'),
        'password':os.getenv('POSTGRES_PASSWORD'),
        'dbname':os.getenv('POSTGRES_DBNAME')
    }

    # Creamos la coneccion a la base de datos
    try:
        conn = pg.connect(**connection)
        print('Conexion realizada con exito!')
        print()
    except Exception as e:
        print(f'Hubo un error al conectarse: {e}')
        print()

    # Si todo sale bien nos retorna la conexion para usarla mas adelante
    return conn    


if __name__ == "__main__":
    conn = conexion_base_de_datos()
    
    # Si la conexion esta abierta la cerramos
    if conn:
        conn.close()
        print('Conexion cerrada')