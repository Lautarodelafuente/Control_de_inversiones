import psycopg2 as pg
import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

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
        logger.info("Conexion a la BD realizada con exito.")
        return conn
    except Exception as e:
        logger.error(f"Error al conectarse a la base de datos: {e}")
        return None  # Retorna None si falla la conexión 


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )
    
    conn = conexion_base_de_datos()
    
    if conn:  # Solo cierra la conexión si fue exitosa
        conn.close()
        logger.info("Conexion a la BD cerrada.")