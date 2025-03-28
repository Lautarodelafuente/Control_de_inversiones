# Librerias
import requests
import pandas as pd
from datetime import datetime
import bd_postgresql as bd
import logging

logger = logging.getLogger(__name__)
#--------------------------------------------------------------------------------#
#API DOLAR CCL

def dolarCCL_api_datos():
    # Define the API URL
    api_url = "https://dolarapi.com/v1/dolares"

    try:
        logger.info("Conectando a la API de Dólar CCL...")
        # Send a GET request
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()  # Lanza un error si el código de estado no es 200
        df = pd.DataFrame(response.json())

        #print(df)

        # Si no hay datos, retornar None
        if df.empty:
            logger.warning("No se obtuvieron datos de la API.")
            return None

        df['fechaActualizacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cotizacion_diaria_dolar = df[df['casa'] == 'contadoconliqui'].reset_index(drop=True)

        logger.info("Datos de la API obtenidos con éxito.")
        return cotizacion_diaria_dolar

    except requests.exceptions.RequestException as e:
        logger.error(f"Error en la solicitud a la API: {e}")
        return None

#--------------------------------------------------------------------------------#
# INSERTAR VALORES DE LA API EN LA BASE DE DATOS

def insert_dolarCCL_api():

    # Nos traemos los datos de la api
    cotizacion_diaria_dolar = dolarCCL_api_datos()

    if cotizacion_diaria_dolar is None:
        logger.error("No se pudo obtener datos de la API. Proceso abortado.")
        return

    # Creamos una tupla con los indices del dataframe.
    cotizacion_data = list(cotizacion_diaria_dolar[['casa', 'nombre', 'compra', 'venta', 'fechaActualizacion']].itertuples(index=False, name=None))

    # Creamos la sentencia SQL para insertar los valores
    insert_query = """
        INSERT INTO public.cotizacion_diaria_dolar (casa, nombre, compra, venta, fechaactualizacion)
        VALUES (%s, %s, %s, %s, %s);
    """

    # Creamos la sentencia SQL para borrar los valores duplicados
    delete_query = """
        DELETE FROM public.cotizacion_diaria_dolar 
        WHERE casa = %s AND nombre = %s AND fechaactualizacion::date = %s::date;
    """
    
    #--------------------------------------------------------------------------------#
    # Conexión con manejo de errores
    try:
        logger.info("Conectando a la base de datos...")
        with bd.conexion_base_de_datos() as conn:
            with conn.cursor() as cur:
                cur.executemany(delete_query, [(casa, nombre, fecha) for casa, nombre, _, _, fecha in cotizacion_data])
                logger.info("Registros duplicados eliminados.")

                cur.executemany(insert_query, cotizacion_data)
                logger.info("Datos insertados en la base de datos.")

            conn.commit()
            logger.info("Commit realizado con éxito.")

    except Exception as e:
        logger.error(f"Error en la base de datos: {e}")

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )

    insert_dolarCCL_api()    