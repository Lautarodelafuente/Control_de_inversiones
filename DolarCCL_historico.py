import time
import logging

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import bd_postgresql as bd
from Services.scraping import iniciar_scraping


logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------------------------------------------------------------------------#

def obtener_dolar(driver):

    try:
        logger.info("Buscando la tabla de cotizaciones del dolar...")
        # Ubicamos el elemento tbody de la tabla de dolar historico con XPATH (camino html para ubicar el elemento). Busca solo el contenido, no las etiquetas
        elemento_tabla = driver.find_element(By.XPATH, '//tbody[@class="general-historical__tbody tbody"]')

        # Extraemos los elementos del body de la tabla correspondiente a cada uno de los registros de la misma. (con el punto le indicamos que tiene que buscar dentro del tbody y no en todo el html)
        elemento_tabla_dolar = elemento_tabla.find_elements(By.XPATH, './/tr')

        logger.info(f"Se encontraron {len(elemento_tabla_dolar)} registros en la tabla.")

        # Creamos una copia para manipularla con libertad y no generar errores a futuro
        dolar_historico = []

        # Del campo referencia cambiamos la coma ',' por punto '.' para luego modificar el tipo de dato de str a float y guardamos la info en la variable dolar_historico. En este punto ya tenemos lista la informacion para pasarla a la base de datos
        for fila in elemento_tabla_dolar:
            fecha, valor = fila.text.split()
            dolar_historico.append([fecha, float(valor.replace(",", "."))])

        logger.info("Datos de la tabla procesados correctamente.")

    except Exception as e:
        logger.error(f"Error al obtener datos del dolar: {e}")
        dolar_historico = []

    finally:
        driver.quit()
        logger.info("Driver de Selenium cerrado correctamente.")

    return dolar_historico

#-----------------------------------------------------------------------------------------------------------------------------------------------#

def ultima_fecha():

    query = "SELECT MAX(fecha)::date FROM dolar_ccl_historico;"
    
    try:
        with bd.conexion_base_de_datos() as conn, conn.cursor() as cur:
            cur.execute(query)
            ultima_fecha_bd = cur.fetchone()[0]
            
            if ultima_fecha_bd:
                ultima_fecha = ultima_fecha_bd.strftime('%d/%m/%Y')
                logger.info(f"Última fecha registrada en BD: {ultima_fecha}")
            else:
                ultima_fecha = "01/01/2000"  # Fecha por defecto si no hay registros en la BD

    except Exception as e:
        logger.error(f"Error al obtener la última fecha de la BD: {e}")
        ultima_fecha = "01/01/2000"

    return ultima_fecha

#--------------------------------------------------------------------------------#
# INSERTAR DATOS EN LA BASE DE DATOS
def info_dolar_to_database(info_dolar):
    upsert_query = """
        INSERT INTO public.dolar_ccl_historico (fecha, referencia, fecha_actualizacion)
        VALUES (%s, %s, NOW())
        ON CONFLICT (fecha) DO NOTHING;
    """
    try:
        with bd.conexion_base_de_datos() as conn, conn.cursor() as cur:
            cur.executemany(upsert_query, info_dolar)
            conn.commit()
            logger.info(f"{len(info_dolar)} registros insertados o actualizados en la BD.")

    except Exception as e:
        logger.error(f"Error al insertar registros en la BD: {e}")
    
#-----------------------------------------------------------------------------------------------------------------------------------------------#

def proceso_gral(url):

    logger.info("Iniciando proceso de scraping...")

    driver = iniciar_scraping()

    # Con el driver previamente cargado, se abre chrome, y le pasamos la url que queremos que entre 
    driver.get(url)

    time.sleep(10)

    # Creamos la variable demora
    delay = 10

    # Corremos el proceso
    try:
        
        # Buscamos la fecha mas reciente de nuestra base de datos para usarla como fecha de inicio en la pagina
        ultima_fecha_registrada = ultima_fecha()
        
        # Configuramos la fecha desde
        WebDriverWait(driver, delay).until(
            EC.element_to_be_clickable((By.XPATH, "//div[@class='general-historical__config config']/input[@class='general-historical__datepicker datepicker desde form-control']"))
            ).send_keys(ultima_fecha_registrada)
        logger.info("Fecha de consulta modificada con exito.")

        time.sleep(10)

        # Clickeamos el boton "VER DATOS" para hacer efectiva nuestra seleccion
        driver.find_element(By.XPATH,'//button[@class="general-historical__button boton"]').click()
        logger.info("Click en VER DATOS realizado.")

        time.sleep(10)

        # Introducimos una demora en el proceso hasta que ubique el elemento deseado para luego continuar
        tabla = WebDriverWait(driver,delay).until(
            EC.presence_of_element_located((By.XPATH,'//tbody[@class="general-historical__tbody tbody"]')))
        logger.info("La pagina cargo correctamente.")

        time.sleep(5)

        # Esperamos tener respuesta del elemento "tabla" o que el programa lo encuentre
        info_dolar = obtener_dolar(driver)

        if info_dolar:
            info_dolar_to_database(info_dolar)
        else:
            logger.warning("No se encontraron registros nuevos.")

    except TimeoutException:
        logger.error("Error: la pagina tardo demasiado en cargar.")
    except Exception as e:
        logger.error(f"Ocurrio un error inesperado en el proceso: {e}")
    


#-----------------------------------------------------------------------------------------------------------------------------------------------#

if __name__ == "__main__":

    #--------------------------------------------------------------------------------#
    # CONFIGURACIoN DE LOGGING
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()] 
    )
    
    logging.info("Iniciando script...")

    # Iniciamos el proceso pasandole el link de la url que queremos scrapear
    proceso_gral('https://www.ambito.com/contenidos/dolar-cl-historico.html')

    logging.info("Fin del proceso.")