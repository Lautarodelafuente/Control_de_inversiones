from selenium.webdriver.common.by import By
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
        
#-----------------------------------------------------------------------------------------------------------------------------------------------#

def obtener_cedears(driver, url='https://www.rava.com/cotizaciones/cedears'):

    # Con el driver previamente cargado, se abre chrome, y le pasamos la url que queremos que entre 
    driver.get(url)

    time.sleep(15)

    # Ubicamos el elemento tbody de la tabla de dolar historico con XPATH (camino html para ubicar el elemento). Busca solo el contenido, no las etiquetas
    try:
        elemento_tabla = driver.find_element(By.XPATH, '//table[@id="table"]')
    except Exception as e:
        logger.error(f"No se pudo encontrar la tabla: {e}")
        return []

    # Extraemos los elementos del body de la tabla correspondiente a cada uno de los registros de la misma. (con el punto le indicamos que tiene que buscar dentro del tbody y no en todo el html)
    filas = elemento_tabla.find_elements(By.XPATH, './/tr')

    logger.info(f'Se encontraron {len(filas)} registros en la tabla de cedears')

    # Creamos una lista para guardar la informacion extraida de la web
    cotizacion_cedear_diaria = []

    # Para cada elemento o fila le pedimos qe nos extraiga el texto con .text y separamos el string por espacios para que nos deje dos elementos correspondientes a fecha y referencia (precio del dolar). Luego lo agregamos a la lista "dolar_historico_base" que creamos anteriormente.
    for fila in filas:
        texto = fila.text
        lista_separada = texto.split()
        cotizacion_cedear_diaria.append(lista_separada)

    # Eliminamos la primer fila de nombres
    cotizacion_cedear_diaria.pop(0)

    #logger.info(cotizacion_cedear_diaria)

    # Aplicamos la conversion de datos
    for i in range(len(cotizacion_cedear_diaria)):
        for j in range(1, len(cotizacion_cedear_diaria[i])):
            cotizacion_cedear_diaria[i][j] = convertir_a_float(cotizacion_cedear_diaria[i][j])

    #logger.info(cotizacion_cedear_diaria)
    return cotizacion_cedear_diaria



def obtener_acciones_lider(driver,url='https://www.rava.com/cotizaciones/acciones-argentinas'):
    
    # Con el driver previamente cargado, se abre chrome, y le pasamos la url que queremos que entre 
    driver.get(url)

    time.sleep(15)

    # Clickeamos el boton "GENERAL" para cambiar al grafico de cotizacion del cuadro general
    driver.find_element(By.XPATH,'/html/body/div[1]/main/div/div/div[2]/div/div/ul/li[1]/a').click()

    time.sleep(5)

    # Ubicamos el elemento tbody de la tabla de dolar historico con XPATH (camino html para ubicar el elemento). Busca solo el contenido, no las etiquetas
    try:
        elemento_tabla_lider = driver.find_element(By.XPATH, '//table[@id="table"]')
    except Exception as e:
        logger.error(f"No se pudo encontrar la tabla: {e}")
        return []

    # Extraemos los elementos del body de la tabla correspondiente a cada uno de los registros de la misma. (con el punto le indicamos que tiene que buscar dentro del tbody y no en todo el html)
    filas_lider = elemento_tabla_lider.find_elements(By.XPATH, './/tr')

    logger.info(f'Se encontraron {len(filas_lider)} registros en la tabla de acciones argentinas lideres')

    # Creamos una lista para guardar la informacion extraida de la web
    cotizacion_acciones_arg_diaria_lider = []

    # Para cada elemento o fila le pedimos qe nos extraiga el texto con .text y separamos el string por espacios para que nos deje dos elementos correspondientes a fecha y referencia (precio del dolar). Luego lo agregamos a la lista "dolar_historico_base" que creamos anteriormente.
    for fila_lider in filas_lider[1:]:
        texto_lider = fila_lider.text
        lista_separada_lider = texto_lider.split()
        lista_separada_lider.append('Lider')
        cotizacion_acciones_arg_diaria_lider.append(lista_separada_lider)

    
    # Aplicamos la conversion de datos
    for i in range(len(cotizacion_acciones_arg_diaria_lider)):
        for j in range(1, len(cotizacion_acciones_arg_diaria_lider[i])):  # Desde el segundo elemento
            cotizacion_acciones_arg_diaria_lider[i][j] = convertir_a_float(cotizacion_acciones_arg_diaria_lider[i][j])

    #cotizacion_acciones_arg_diaria_lider.append('Lider')

    #logger.info(cotizacion_acciones_arg_diaria_lider)

    return cotizacion_acciones_arg_diaria_lider


def obtener_acciones_general(driver,url='https://www.rava.com/cotizaciones/acciones-argentinas'):
    
    # Con el driver previamente cargado, se abre chrome, y le pasamos la url que queremos que entre 
    driver.get(url)

    time.sleep(15)
    
    # Clickeamos el boton "GENERAL" para cambiar al grafico de cotizacion del cuadro general
    driver.find_element(By.XPATH,'/html/body/div[1]/main/div/div/div[2]/div/div/ul/li[2]/a').click()
    
    time.sleep(5)

    # Ubicamos el elemento tbody de la tabla de dolar historico con XPATH (camino html para ubicar el elemento). Busca solo el contenido, no las etiquetas
    try:
        elemento_tabla_general = driver.find_element(By.XPATH, '//table[@id="table"]')
    except Exception as e:
        logger.error(f"No se pudo encontrar la tabla: {e}")
        return []

    # Extraemos los elementos del body de la tabla correspondiente a cada uno de los registros de la misma. (con el punto le indicamos que tiene que buscar dentro del tbody y no en todo el html)
    filas_general = elemento_tabla_general.find_elements(By.XPATH, './/tr')

    logger.info(f'Se encontraron {len(filas_general)} registros en la tabla acciones argentinas general')

    cotizacion_acciones_arg_diaria_general = []

    # Para cada elemento o fila le pedimos qe nos extraiga el texto con .text y separamos el string por espacios para que nos deje dos elementos correspondientes a fecha y referencia (precio del dolar). Luego lo agregamos a la lista "dolar_historico_base" que creamos anteriormente.
    for fila_general in filas_general[1:]:
        texto_general = fila_general.text
        lista_separada_general = texto_general.split()
        lista_separada_general.append('General')
        cotizacion_acciones_arg_diaria_general.append(lista_separada_general)
            
    # Aplicamos la conversion de datos a todas las filas
    for i in range(len(cotizacion_acciones_arg_diaria_general)):
        for j in range(1, len(cotizacion_acciones_arg_diaria_general[i])):  # Desde el segundo elemento
            cotizacion_acciones_arg_diaria_general[i][j] = convertir_a_float(cotizacion_acciones_arg_diaria_general[i][j])

    #cotizacion_acciones_arg_diaria_general.append('General')

    #logger.info(cotizacion_acciones_arg_diaria_general)
 
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
                        cur.execute(query, data)
                        conteo += 1 
                    except Exception as e:
                        logger.error(f"Error al insertar el registro {data}: {e}")
                        continue  # Continúa con el siguiente registro

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
        porcentaje_gan_año,
        cotizacion_anterior,
        cotizacion_apertura,
        cotizacion_minimo,
        cotizacion_maximo,
        hora,
        vol_nominal,
        vol_efectivo,
        ratio,
        ccl)
    VALUES(%s, %s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker)
    DO update set 
        ticker = EXCLUDED.ticker
        , ultima_cotizacion = EXCLUDED.ultima_cotizacion
        , porcentaje_gan_dia = EXCLUDED.porcentaje_gan_dia
        , porcentaje_gan_mes = EXCLUDED.porcentaje_gan_mes
        , porcentaje_gan_año = EXCLUDED.porcentaje_gan_año
        , cotizacion_anterior = EXCLUDED.cotizacion_anterior
        , cotizacion_apertura = EXCLUDED.cotizacion_apertura
        , cotizacion_minimo = EXCLUDED.cotizacion_minimo
        , cotizacion_maximo = EXCLUDED.cotizacion_maximo
        , hora = EXCLUDED.hora
        , vol_nominal = EXCLUDED.vol_nominal
        , vol_efectivo = EXCLUDED.vol_efectivo
        , ratio = EXCLUDED.ratio
        , ccl = EXCLUDED.ccl
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
        porcentaje_gan_año,
        cotizacion_anterior,
        cotizacion_apertura,
        cotizacion_minimo,
        cotizacion_maximo,
        hora,
        vol_nominal,
        vol_efectivo,
        tipo_accion)
    VALUES(%s, %s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker)
    DO update set 
        ticker = EXCLUDED.ticker
        , ultima_cotizacion = EXCLUDED.ultima_cotizacion
        , porcentaje_gan_dia = EXCLUDED.porcentaje_gan_dia
        , porcentaje_gan_mes = EXCLUDED.porcentaje_gan_mes
        , porcentaje_gan_año = EXCLUDED.porcentaje_gan_año
        , cotizacion_anterior = EXCLUDED.cotizacion_anterior
        , cotizacion_apertura = EXCLUDED.cotizacion_apertura
        , cotizacion_minimo = EXCLUDED.cotizacion_minimo
        , cotizacion_maximo = EXCLUDED.cotizacion_maximo
        , hora = EXCLUDED.hora
        , vol_nominal = EXCLUDED.vol_nominal
        , vol_efectivo = EXCLUDED.vol_efectivo
        ,tipo_accion = EXCLUDED.tipo_accion
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
        porcentaje_gan_año,
        cotizacion_anterior,
        cotizacion_apertura,
        cotizacion_minimo,
        cotizacion_maximo,
        hora,
        vol_nominal,
        vol_efectivo,
        tipo_accion)
    VALUES(%s, %s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
    ON CONFLICT (ticker)
    DO update set 
        ticker = EXCLUDED.ticker
        , ultima_cotizacion = EXCLUDED.ultima_cotizacion
        , porcentaje_gan_dia = EXCLUDED.porcentaje_gan_dia
        , porcentaje_gan_mes = EXCLUDED.porcentaje_gan_mes
        , porcentaje_gan_año = EXCLUDED.porcentaje_gan_año
        , cotizacion_anterior = EXCLUDED.cotizacion_anterior
        , cotizacion_apertura = EXCLUDED.cotizacion_apertura
        , cotizacion_minimo = EXCLUDED.cotizacion_minimo
        , cotizacion_maximo = EXCLUDED.cotizacion_maximo
        , hora = EXCLUDED.hora
        , vol_nominal = EXCLUDED.vol_nominal
        , vol_efectivo = EXCLUDED.vol_efectivo
        ,tipo_accion = EXCLUDED.tipo_accion
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