from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import bd_postgresql as bd
from Services.scraping import iniciar_scraping
import logging

logger = logging.getLogger(__name__)

# Funcion para convertir valores
def convertir_a_float(valor):
    try:
        if valor == '-':
            return 0
        return float(valor.replace(".", '').replace(",", "."))
    except ValueError:
        #logging.warning(f"Valor no numerico encontrado: {valor}")
        return valor
    
def convertir_volumen(valor):
    if valor == '-':
        return 0
    valor = valor.strip()
    multiplicador = 1
    if valor.endswith('MM'):
        multiplicador = 1_000_000
        valor = valor[:-2].strip()
    elif valor.endswith('M'):
        multiplicador = 1_000
        valor = valor[:-1].strip()
    elif valor.endswith('K'):
        multiplicador = 1_000
        valor = valor[:-1].strip()
    try:
        numero = float(valor.replace(".", '').replace(",", "."))
        return int(round(numero * multiplicador))
    except ValueError:
        return valor
        
#-----------------------------------------------------------------------------------------------------------------------------------------------#

def obtener_cedears(driver, url='https://www.rava.com/cotizaciones/cedears'):

    # Con el driver previamente cargado, se abre chrome, y le pasamos la url que queremos que entre 
    driver.get(url)

    time.sleep(15)

    '''driver.save_screenshot('debug_cedears.png')
    with open('debug_cedears.html', 'w', encoding='utf-8') as f:
        f.write(driver.page_source)'''

    # Ubicamos el elemento tbody de la tabla de dolar historico con XPATH (camino html para ubicar el elemento). Busca solo el contenido, no las etiquetas
    try:
        # 1. Esperar a que el iframe exista y cambiar el contexto hacia él
        WebDriverWait(driver, 20).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "embed-frame-cedears"))
        )

        # 2. Ahora sí, buscar la tabla DENTRO del iframe
        elemento_tabla = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, '//table'))
        )
        print(f'Elemento tabla encontrado: {elemento_tabla}')

    except TimeoutException as e:
        logger.error(f"No se pudo encontrar la tabla (timeout): {e}")
        driver.switch_to.default_content()
        return []
    except Exception as e:
        logger.error(f"No se pudo encontrar la tabla: {e}")
        driver.switch_to.default_content()
        return []

    '''with open('debug_tabla_iframe.html', 'w', encoding='utf-8') as f:
        f.write(elemento_tabla.get_attribute('outerHTML'))

    filas_debug = elemento_tabla.find_elements(By.XPATH, './/tr')
    with open('debug_fila_ejemplo.html', 'w', encoding='utf-8') as f:
        f.write(filas_debug[6].get_attribute('outerHTML'))  # fila índice 2 para saltar el header'''

    # Extraemos los elementos del body de la tabla correspondiente a cada uno de los registros de la misma. (con el punto le indicamos que tiene que buscar dentro del tbody y no en todo el html)
    filas = elemento_tabla.find_elements(By.XPATH, './/tbody/tr')
    logger.info(f'Se encontraron {len(filas)} registros en la tabla de cedears')

    cotizacion_cedear_diaria = []

    for fila in filas:
        celdas = fila.find_elements(By.TAG_NAME, 'td')
        if len(celdas) < 13:
            continue

        ticker = celdas[1].text.strip()
        nombre_largo = celdas[2].get_attribute('title') or celdas[2].text.strip()
        ultima_cotizacion = convertir_a_float(celdas[3].text.strip())
        pct_dia = convertir_a_float(celdas[4].text.strip())
        pct_mes = convertir_a_float(celdas[5].text.strip())
        pct_anio = convertir_a_float(celdas[6].text.strip())
        vol_nominal = convertir_volumen(celdas[7].text.strip())
        vol_efectivo = convertir_volumen(celdas[8].text.strip())
        ratio = celdas[11].text.strip()  # se queda como texto, ej "20:1"
        ccl = convertir_a_float(celdas[12].text.strip())

        registro = [
            ticker, ultima_cotizacion, pct_dia, pct_mes, pct_anio,
            vol_nominal, vol_efectivo, ratio, ccl, nombre_largo
        ]
        cotizacion_cedear_diaria.append(registro)

    driver.switch_to.default_content()

    return cotizacion_cedear_diaria



def obtener_acciones_lider(driver, url='https://www.rava.com/cotizaciones/acciones-argentinas'):
    driver.get(url)
    time.sleep(15)

    try:
        WebDriverWait(driver, 20).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "embed-frame-acciones-argentinas"))
        )
        elemento_tabla = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//span[text()='Panel Líder (Merval)']/ancestor::div[contains(@class,'flex-col')][1]//table"
            ))
        )
    except TimeoutException as e:
        logger.error(f"No se pudo encontrar la tabla de acciones lider (timeout): {e}")
        driver.switch_to.default_content()
        return []
    except Exception as e:
        logger.error(f"No se pudo encontrar la tabla de acciones lider: {e}")
        driver.switch_to.default_content()
        return []

    filas_lider = elemento_tabla.find_elements(By.XPATH, './/tbody/tr')
    logger.info(f'Se encontraron {len(filas_lider)} registros en la tabla de acciones argentinas lideres')

    cotizacion_acciones_arg_diaria_lider = []

    for fila in filas_lider:
        celdas = fila.find_elements(By.TAG_NAME, 'td')
        if len(celdas) < 11:
            continue

        ticker = celdas[1].text.strip()
        ultima_cotizacion = convertir_a_float(celdas[3].text.strip())
        pct_dia = convertir_a_float(celdas[4].text.strip())
        pct_mes = convertir_a_float(celdas[5].text.strip())
        pct_anio = convertir_a_float(celdas[6].text.strip())
        vol_nominal = convertir_volumen(celdas[7].text.strip())
        vol_efectivo = convertir_volumen(celdas[8].text.strip())

        registro = [ticker, ultima_cotizacion, pct_dia, pct_mes, pct_anio, vol_nominal, vol_efectivo, 'Lider']
        cotizacion_acciones_arg_diaria_lider.append(registro)

    driver.switch_to.default_content()
    return cotizacion_acciones_arg_diaria_lider


def obtener_acciones_general(driver, url='https://www.rava.com/cotizaciones/acciones-argentinas'):
    driver.get(url)
    time.sleep(15)

    try:
        WebDriverWait(driver, 20).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "embed-frame-acciones-argentinas"))
        )
        elemento_tabla = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//span[text()='Panel General']/ancestor::div[contains(@class,'flex-col')][1]//table"
            ))
        )
    except TimeoutException as e:
        logger.error(f"No se pudo encontrar la tabla de acciones general (timeout): {e}")
        driver.switch_to.default_content()
        return []
    except Exception as e:
        logger.error(f"No se pudo encontrar la tabla de acciones general: {e}")
        driver.switch_to.default_content()
        return []

    filas_general = elemento_tabla.find_elements(By.XPATH, './/tbody/tr')
    logger.info(f'Se encontraron {len(filas_general)} registros en la tabla acciones argentinas general')

    cotizacion_acciones_arg_diaria_general = []

    for fila in filas_general:
        celdas = fila.find_elements(By.TAG_NAME, 'td')
        if len(celdas) < 11:
            continue

        ticker = celdas[1].text.strip()
        ultima_cotizacion = convertir_a_float(celdas[3].text.strip())
        pct_dia = convertir_a_float(celdas[4].text.strip())
        pct_mes = convertir_a_float(celdas[5].text.strip())
        pct_anio = convertir_a_float(celdas[6].text.strip())
        vol_nominal = convertir_volumen(celdas[7].text.strip())
        vol_efectivo = convertir_volumen(celdas[8].text.strip())

        registro = [ticker, ultima_cotizacion, pct_dia, pct_mes, pct_anio, vol_nominal, vol_efectivo, 'General']
        cotizacion_acciones_arg_diaria_general.append(registro)

    driver.switch_to.default_content()
    return cotizacion_acciones_arg_diaria_general


#-----------------------------------------------------------------------------------------------------------------------------------------------#


def Carga_cotizacion_bd(cotizacion_list=None, query=None):

    if cotizacion_list is None:
        logger.error("La lista de cotizaciones no puede ser None")
        return
    
    if query is None:
        logger.error("La consulta SQL no puede ser None")
        return

    try:
        # Conexion y manejo de la base de datos con 'with' para asegurar que se cierre automáticamente
        with bd.conexion_base_de_datos() as conn:
            with conn.cursor() as cur:
                conteo = 0

                for data in cotizacion_list:
                    try:
                        cur.execute("SAVEPOINT sp_registro")
                        cur.execute(query, data)
                        cur.execute("RELEASE SAVEPOINT sp_registro")
                        conteo += 1
                    except Exception as e:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_registro")
                        logger.error(f"Error al insertar el registro {data}: {e}")
                        continue

                # Commit de los cambios
                conn.commit()

                logger.info(f'Se revisaron, insertaron o actualizaron {conteo} registros con exito.')
        
        conn.close()

    except Exception as e:
        logger.error(f'Error en la conexion o transaccion: {e}')
    

#-----------------------------------------------------------------------------------------------------------------------------------------------#

# Creamos la sentencia SQL
cedear_rava_upsert_query = """
    INSERT INTO public.rava_cotizacion_cedear_diaria
        (ticker,
        ultima_cotizacion,
        porcentaje_gan_dia,
        porcentaje_gan_mes,
        porcentaje_gan_anio,
        vol_nominal,
        vol_efectivo,
        ratio,
        ccl,
        nombre_largo)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker)
    DO UPDATE SET
        ultima_cotizacion = EXCLUDED.ultima_cotizacion
        , porcentaje_gan_dia = EXCLUDED.porcentaje_gan_dia
        , porcentaje_gan_mes = EXCLUDED.porcentaje_gan_mes
        , porcentaje_gan_anio = EXCLUDED.porcentaje_gan_anio
        , vol_nominal = EXCLUDED.vol_nominal
        , vol_efectivo = EXCLUDED.vol_efectivo
        , ratio = EXCLUDED.ratio
        , ccl = EXCLUDED.ccl
        , nombre_largo = EXCLUDED.nombre_largo
    ;
"""

#-----------------------------------------------------------------------------------------------------------------------------------------------#
# Creamos la sentencia SQL
acciones_lider_rava_upsert_query = """
    INSERT INTO public.rava_cotizacion_acciones_arg_lider
        (ticker,
        ultima_cotizacion,
        porcentaje_gan_dia,
        porcentaje_gan_mes,
        porcentaje_gan_anio,
        vol_nominal,
        vol_efectivo,
        tipo_accion)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker)
    DO UPDATE SET
        ultima_cotizacion = EXCLUDED.ultima_cotizacion
        , porcentaje_gan_dia = EXCLUDED.porcentaje_gan_dia
        , porcentaje_gan_mes = EXCLUDED.porcentaje_gan_mes
        , porcentaje_gan_anio = EXCLUDED.porcentaje_gan_anio
        , vol_nominal = EXCLUDED.vol_nominal
        , vol_efectivo = EXCLUDED.vol_efectivo
        , tipo_accion = EXCLUDED.tipo_accion
    ;
"""

#-----------------------------------------------------------------------------------------------------------------------------------------------#

# Creamos la sentencia SQL
acciones_gral_rava_upsert_query = """
    INSERT INTO public.rava_cotizacion_acciones_arg_gral
        (ticker,
        ultima_cotizacion,
        porcentaje_gan_dia,
        porcentaje_gan_mes,
        porcentaje_gan_anio,
        vol_nominal,
        vol_efectivo,
        tipo_accion)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker)
    DO UPDATE SET
        ultima_cotizacion = EXCLUDED.ultima_cotizacion
        , porcentaje_gan_dia = EXCLUDED.porcentaje_gan_dia
        , porcentaje_gan_mes = EXCLUDED.porcentaje_gan_mes
        , porcentaje_gan_anio = EXCLUDED.porcentaje_gan_anio
        , vol_nominal = EXCLUDED.vol_nominal
        , vol_efectivo = EXCLUDED.vol_efectivo
        , tipo_accion = EXCLUDED.tipo_accion
    ;
"""

#-----------------------------------------------------------------------------------------------------------------------------------------------#
   
def proceso_general_rava():
    
    driver = iniciar_scraping()
    #-----------------------------------------------------------------------------------------------------------------------------------------------#
    # FUNCIONES

    cotizacion_cedear = obtener_cedears(driver=driver,url='https://www.rava.com/cotizaciones/cedears')

    cotizacion_acciones_lider = obtener_acciones_lider(driver=driver, url='https://www.rava.com/cotizaciones/acciones-argentinas')

    cotizacion_acciones_gral = obtener_acciones_general(driver=driver, url='https://www.rava.com/cotizaciones/acciones-argentinas')

    #-----------------------------------------------------------------------------------------------------------------------------------------------#
    # CERRAMOS EL PROCESO
    # Cerramos el driver de chrome
    driver.close()

    Carga_cotizacion_bd(cotizacion_list=cotizacion_cedear,query=cedear_rava_upsert_query)
    Carga_cotizacion_bd(cotizacion_list=cotizacion_acciones_lider,query=acciones_lider_rava_upsert_query)
    Carga_cotizacion_bd(cotizacion_list=cotizacion_acciones_gral,query=acciones_gral_rava_upsert_query)
    

#-----------------------------------------------------------------------------------------------------------------------------------------------#
# INICIAMOS EL PROCESO

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    proceso_general_rava() 