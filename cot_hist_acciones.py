import logging
import time
import os

import bd_postgresql as bd
from Services.scraping import iniciar_scraping_with_download, iniciar_scraping

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd
import re


logger = logging.getLogger(__name__)  # Crea un logger específico para este módulo

def obtener_tickers(tipo):
    logger.info(f"Obteniendo tickers de {tipo} de la base de datos...")
    
    conn = bd.conexion_base_de_datos()
    if not conn:
        logger.error("Error: No se pudo establecer conexión con la base de datos.")
        return []
    
    tipo = tipo.upper()

    query_ticker_acciones = '''SELECT DISTINCT simbolo FROM iol_portafolio_actual WHERE tipo = %s;'''

    try:
        with conn.cursor() as cur:  # Manejo automático del cursor
            cur.execute(query_ticker_acciones,(tipo,))
            ticker_acciones = cur.fetchall()
            tkr = [e[0] for e in ticker_acciones] if ticker_acciones else []
            logger.info(f"EXITO! Se obtuvieron los {len(ticker_acciones)} tickers correctamente.")
        
        if not tkr:
            logger.warning(f"No se encontraron tickers en la base para el tipo {tipo}.")
            return []
        
        return tkr  # Retornamos la lista de tickers

    except Exception as e:
        logger.error(f'Error al buscar los tickers del tipo {tipo}: {e}')
        return []
    
    finally:
        conn.close()


def cot_accion_hist(ticker):
    logger.info(f"Obteniendo historico de {ticker}...")

    logger.info(os.getcwd())

    url = f'https://www.rava.com/perfil/{ticker}'

    file_path = f"./Download/{ticker} - Cotizaciones historicas.csv"
    
    # Eliminar archivo existente
    if os.path.exists(file_path):
        os.remove(file_path)
        logger.info(f"Archivo eliminado: {file_path}")

    driver = iniciar_scraping_with_download()

    try:
        driver.get(url)

        WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH,'/html/body/div[1]/main/div/div/div[3]/div[2]/div/div/div[3]/button')
                )).click()
        
        # Esperar hasta que el archivo aparezca en la carpeta
        WebDriverWait(driver, 30).until(lambda _: os.path.exists(file_path))
        
        logger.info(f'Archivo {file_path} descargado con exito!')

    except TimeoutException:
        logger.error(f"Tiempo de espera agotado al descargar {ticker}.")
    except Exception as e:
        logger.error(f"Error inesperado en la descarga de {ticker}: {e}")
    finally:
        driver.quit()


def obtener_fecha_inicio(conn, ticker):
    """ Obtiene la fecha de inicio de inversión para el ticker. """
    query = """
        SELECT 
            GREATEST(MIN(fechaorden)::date, COALESCE(MAX(c.fecha), '2019-08-01'))
        FROM iol_operaciones_historicas ioh 
        LEFT JOIN cotizacion_historica_accion c ON c.ticker = ioh.simbolo 
        WHERE simbolo = %s;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (ticker,))
            return pd.to_datetime(cur.fetchone()[0])
    except Exception as e:
        logger.error(f"Error al obtener fecha de inicio para {ticker}: {e}")
        return pd.to_datetime('2019-08-01')
    

def cot_accion_hist_bd(ticker):

    file_path = f"./Download/{ticker} - Cotizaciones historicas.csv"

    if not os.path.exists(file_path):
        logger.error(f"No se encontró el archivo para {ticker}.")
        return
    
    conn = bd.conexion_base_de_datos()
    if not conn:
        logger.error("Error: No se pudo establecer conexión con la base de datos.")

    try:
        # Cargar CSV
        df = pd.read_csv(file_path, usecols=["especie", "fecha", "apertura", "maximo", "minimo", "cierre", "volumen"])
        df["fecha"] = pd.to_datetime(df["fecha"]) 
        
        fecha_inicio = obtener_fecha_inicio(conn, ticker)

        datos = [tuple(x) for x in (df[df["fecha"]>= fecha_inicio]).to_numpy()]

        with conn.cursor() as cur:
            cur.execute("""
                create table if not exists cotizacion_historica_accion (
                    ticker varchar(10),
                    fecha date ,
                    apertura numeric(10,2),
                    maximo numeric(10,2),
                    minimo numeric(10,2),
                    cierre numeric(10,2),
                    volumen numeric(10,2)
                );
            """)

            cur.execute("""
                CREATE UNIQUE INDEX if not exists cotizacion_historica_accion_ticker_idx ON public.cotizacion_historica_accion (ticker,fecha);
            """)

            conn.commit()

        if not datos:
            logger.info(f"No se encontraron registros para {ticker}.")
            return

        query_insert = """
            INSERT INTO public.cotizacion_historica_accion
                (ticker,
                fecha,
                apertura,
                maximo,
                minimo,
                cierre,
                volumen)
            VALUES(%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ticker,fecha)
            DO nothing;
        """
        with conn.cursor() as cur:
            cur.executemany(query_insert, datos)
            conn.commit()

        logger.info(f"Se insertaron {len(datos)} registros para {ticker}.")
    
    except Exception as e:
        logger.exception(f"Error al procesar datos de {ticker}: {e}")
    finally:
        conn.close()


def cot_cedear_hist(ticker):
    logger.info(f"Obteniendo historico de {ticker}...")

    logger.info(os.getcwd())

    url = f'https://es.finance.yahoo.com/quote/{ticker}.BA/history/'

    conn = bd.conexion_base_de_datos()
    if not conn:
        logger.error("Error: No se pudo establecer conexión con la base de datos.")
    
    fecha_inicio = obtener_fecha_inicio(conn,ticker)

    driver = iniciar_scraping()

    try:
        driver.get(url)

        delay = 10

        cookies = WebDriverWait(driver, delay).until(
                EC.element_to_be_clickable(
                    (By.XPATH,'//*[@id="consent-page"]/div/div/div/form/div[2]/div[2]/button[2]')
                ))
        
        if cookies:
            cookies.click()

        time.sleep(delay)

        WebDriverWait(driver, delay).until(
                EC.element_to_be_clickable(
                    (By.XPATH,'/html/body/div[2]/main/section/section/section/article/div[1]/div[1]/div[1]/button')
                )).click()
        
        # Configuramos la fecha desde
        WebDriverWait(driver, delay).until(
            EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/main/section/section/section/article/div[1]/div[1]/div[1]/div/div/section/div[2]/input[1]"))
        ).send_keys(fecha_inicio.strftime('%d-%m-%Y'))
        logger.info("Fecha de consulta modificada con exito.")

        time.sleep(delay)

        if driver.find_elements(By.XPATH, '/html/body/div[2]/main/section/section/section/article/div[1]/div[1]/div[1]/div/div/section[contains(., "La fecha no puede ser anterior a")]'):
            msj_fecha_inicio = driver.find_elements(By.XPATH, '/html/body/div[2]/main/section/section/section/article/div[1]/div[1]/div[1]/div/div/section[contains(., "La fecha no puede ser anterior a")]')

            logger.info(f"Fecha de inicio {fecha_inicio} no válida para {ticker}.")
            for msg in msj_fecha_inicio:
                if 'La fecha no puede ser anterior a' in msg.text:
                    # Extraer la fecha en formato "dd mmm yyyy"
                    fecha_match = re.search(r'\b\d{2} \w{3} \d{4}\b', msg.text)
                    if fecha_match:
                        fecha_extraida = fecha_match.group()
                        fecha_transformada = (pd.to_datetime(fecha_extraida, format='%d %b %Y') + pd.Timedelta(days=1)).strftime('%d-%m-%Y')
                        logger.warning(f"Fecha transformada: {fecha_transformada}")

                        WebDriverWait(driver, delay).until(
                            EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/main/section/section/section/article/div[1]/div[1]/div[1]/div/div/section/div[2]/input[1]"))
                        ).clear()
                        
                        WebDriverWait(driver, delay).until(
                            EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/main/section/section/section/article/div[1]/div[1]/div[1]/div/div/section/div[2]/input[1]"))
                        ).send_keys(fecha_transformada)
                        logger.info("Fecha de consulta modificada con exito.")

                        #time.sleep(delay)
        
        WebDriverWait(driver, delay).until(
                EC.element_to_be_clickable(
                    (By.XPATH,'/html/body/div[2]/main/section/section/section/article/div[1]/div[1]/div[1]/div/div/section/div[3]/button[1]')
                )).click()
        
        time.sleep(3)

        tabla_rows = driver.find_elements(By.XPATH,'/html/body/div[2]/main/section/section/section/article/div[1]/div[3]/table/tbody/tr')

        datos_historicos = []

        for row in tabla_rows:
            datos = row.find_elements(By.XPATH, './/td')
            dato = [x.text for x in datos]
            datos_historicos.append(dato)

        logger.info(f'Datos {ticker} obtenidos con exito! cantidad: {len(datos_historicos)}')
        return datos_historicos
    except TimeoutException:
        logger.error(f"Tiempo de espera agotado al descargar {ticker}.")
    except Exception as e:
        logger.error(f"Error inesperado en la descarga de {ticker}: {e}")
    finally:
        driver.quit()

    

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    #tickers = obtener_tickers('ACCIONES')

    '''for ticker in tickers:
        #tkr = tkr.lower()
        cot_accion_hist(ticker)
        cot_accion_hist_bd(ticker)'''
    
    tickers = obtener_tickers('CEDEARS')

    for ticker in tickers:
        cot_cedear_hist(ticker)