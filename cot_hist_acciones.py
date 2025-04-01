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

    delay = 30

    try:
        driver.get(url)
        
        if WebDriverWait(driver, delay).until(
                EC.element_to_be_clickable(
                    (By.XPATH,'/html/body/div[1]/main/div/div/div[3]/div[3]/div/div/div[3]/button')
                )):
            logger.info("Botón de descarga encontrado.")
            driver.find_element(By.XPATH,'/html/body/div[1]/main/div/div/div[3]/div[3]/div/div/div[3]/button').click()
            logger.info("Botón de descarga clickeado.")
        
        elif WebDriverWait(driver, delay).until(
                EC.element_to_be_clickable(
                    (By.XPATH,'/html/body/div[1]/main/div/div/div[3]/div[2]/div/div/div[3]/button')
                )):
            logger.info("Botón de descarga alternativo encontrado.")
            driver.find_element(By.XPATH,'/html/body/div[1]/main/div/div/div[3]/div[2]/div/div/div[3]/button').click()
            logger.info("Botón de descarga clickeado.")
        
        else:
            logger.error(f"No se encontró el botón de descarga en {url}.")
            return
        
        # Esperar hasta que el archivo aparezca en la carpeta
        WebDriverWait(driver, delay).until(lambda _: os.path.exists(file_path))
        
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
            return (pd.to_datetime(cur.fetchone()[0]) - pd.Timedelta(days=7))
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
            DO UPDATE SET
                apertura = EXCLUDED.apertura,
                maximo = EXCLUDED.maximo,
                minimo = EXCLUDED.minimo,
                cierre = EXCLUDED.cierre,
                volumen = EXCLUDED.volumen;
        """

        # Insertar datos en la base de datos
        with conn.cursor() as cur:
            cur.executemany(query_insert, datos)
            conn.commit()

        logger.info(f"Se insertaron {len(datos)} registros para {ticker}.")
    
    except Exception as e:
        logger.exception(f"Error al procesar datos de {ticker}: {e}")
    finally:
        conn.close()


def cot_cedear_hist(tickers):
    '''
    Obtiene el historico de cotizaciones de los CEDEARS desde Yahoo Finance y lo guarda en la base de datos.
    '''
    conn = bd.conexion_base_de_datos()
    if not conn:
        logger.error("Error: No se pudo establecer conexión con la base de datos.")
        return []
    
    driver = iniciar_scraping()

    yahoo_cookies = 0
    datos_historicos = []

    try:        
        for ticker in tickers:
            logger.info(f"Procesando ticker: {ticker}...")
            
            url = f'https://es.finance.yahoo.com/quote/{ticker}.BA/history/'
            fecha_inicio = obtener_fecha_inicio(conn,ticker)

            driver.get(url)

            delay = 20

            if yahoo_cookies == 0:
                # Esperar a que el botón de cookies sea clickeable y hacer clic en él
                logger.info("Esperando el botón de cookies...")
                try:
                    cookies = WebDriverWait(driver, delay).until(
                        EC.element_to_be_clickable(
                            (By.XPATH, '//*[@id="consent-page"]/div/div/div/form/div[2]/div[2]/button[2]')
                        )
                    )
                    if cookies:
                        cookies.click()
                        yahoo_cookies = 1
                        logger.info("Botón de cookies clickeado.")
                    
                except TimeoutException:
                    logger.warning(f"No se encontró el botón de cookies para {ticker}.")

            time.sleep(5)

            WebDriverWait(driver, delay).until(
                EC.element_to_be_clickable(
                    (By.XPATH,'/html/body/div[2]/main/section/section/section/article/div[1]/div[1]/div[1]/button')
                )).click()
            
            # Configuramos la fecha desde
            WebDriverWait(driver, delay).until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/main/section/section/section/article/div[1]/div[1]/div[1]/div/div/section/div[2]/input[1]"))
            ).send_keys(fecha_inicio.strftime('%d-%m-%Y'))
            
            time.sleep(delay)

            if driver.find_elements(By.XPATH, '/html/body/div[2]/main/section/section/section/article/div[1]/div[1]/div[1]/div/div/section[contains(., "La fecha no puede ser anterior a")]'):
                msj_fecha_inicio = driver.find_elements(By.XPATH, '/html/body/div[2]/main/section/section/section/article/div[1]/div[1]/div[1]/div/div/section[contains(., "La fecha no puede ser anterior a")]')

                logger.warning(f"Fecha de inicio {fecha_inicio} no válida para {ticker}.")

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
            
            WebDriverWait(driver, delay).until(
                EC.element_to_be_clickable(
                    (By.XPATH,'/html/body/div[2]/main/section/section/section/article/div[1]/div[1]/div[1]/div/div/section/div[3]/button[1]')
                )).click()
            
            time.sleep(0.5)

            WebDriverWait(driver, delay).until(
                EC.presence_of_element_located(
                    (By.XPATH,'/html/body/div[2]/main/section/section/section/article/div[1]/div[3]/table/tbody/tr')
                ))
            tabla_rows = driver.find_elements(By.XPATH,'/html/body/div[2]/main/section/section/section/article/div[1]/div[3]/table/tbody/tr')
            logger.info("Carga tabla completa.")

            # Process table rows
            for row in tabla_rows:
                row_data = row.text.split(sep=' ')
                row_data.append(ticker)  # Add the ticker to the row data
                datos_historicos.append(row_data)

            logger.info(f'Datos {ticker} obtenidos con exito! cantidad: {len(tabla_rows)}')
        
    except TimeoutException:
        logger.error(f"Tiempo de espera agotado al descargar {ticker}.")
    except Exception as e:
        logger.error(f"Error inesperado en la descarga de {ticker}: {e}")
    finally:
        driver.quit()

    return datos_historicos

def cot_cedear_hist_bd(datos_historicos):
    ''' Guarda los datos historicos de cedears en la base de datos. '''

    logger.info("Guardando datos historicos en la base de datos...")

    if not datos_historicos:
        logger.warning("No hay datos para guardar.")
        return []
    
    datos_historicos_v2 = []

    for row in datos_historicos:
        match row[1]:
            case 'ene':
                row[1] = '01'
            case 'feb': 
                row[1] = '02'
            case 'mar':
                row[1] = '03'
            case 'abr':
                row[1] = '04'
            case 'may':
                row[1] = '05'
            case 'jun':
                row[1] = '06'
            case 'jul':
                row[1] = '07'
            case 'ago':
                row[1] = '08'
            case 'sept':
                row[1] = '09'
            case 'oct':
                row[1] = '10'
            case 'nov':
                row[1] = '11'
            case 'dic':
                row[1] = '12'
            case _:
                logger.error(f"Mes no reconocido: {row[1]}")

        try:
            # Skip rows where 'maximo' is 'Dividendo'
            if row[4] != 'Dividendo':
                # Parse the date
                row[0] = f"{row[2]}-{row[1]}-{row[0]}"
                row[0] = pd.to_datetime(row[0], format='%Y-%m-%d').strftime('%Y-%m-%d')
                del row[1:3]  # Remove the now redundant elements

                # Convert numeric fields (replace '.' with '' and ',' with '.')
                for i in range(1, 6):  # Columns: apertura, maximo, minimo, cierre, cierre_ajustado
                    row[i] = float(row[i].replace('.', '').replace(',', '.')) if row[i] != '-' else 0

                # Convert volumen (replace '.' with '' and ',' with '.')
                row[6] = float(row[6].replace('.', '').replace(',', '.')) if row[6] != '-' else 0

                datos_historicos_v2.append(row)  # Append the row if 'maximo' is not 'Dividendo'

        except Exception as e:
            logger.error(f"Error parsing date for row {row}: {e}")   

    # Crear DataFrame
    df = pd.DataFrame(datos_historicos_v2, columns=["fecha", "apertura", "maximo", "minimo", "cierre", "cierre_ajustado", "volumen", "ticker"])
    
    datos = [tuple(x) for x in (df).to_numpy()]

    conn = bd.conexion_base_de_datos()
    if not conn:
        logger.error("Error: No se pudo establecer conexión con la base de datos.")    
        return []
    
    try:
        query_insert = """
            INSERT INTO public.cotizacion_historica_accion
                (
                fecha,
                apertura,
                maximo,
                minimo,
                cierre,
                cierre_ajustado,
                volumen,
                ticker)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ticker,fecha)
            DO UPDATE SET
                apertura = EXCLUDED.apertura,
                maximo = EXCLUDED.maximo,
                minimo = EXCLUDED.minimo,
                cierre = EXCLUDED.cierre,
                cierre_ajustado = EXCLUDED.cierre_ajustado,
                volumen = EXCLUDED.volumen;
        """
        with conn.cursor() as cur:
            cur.executemany(query_insert, datos)
            conn.commit()

        logger.info(f"Se insertaron {len(datos)} registros de cedears.")
    
    except Exception as e:
        logger.exception(f"Error al procesar datos historicos de cedears: {e}")
    finally:
        conn.close()

  

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    tickers_accion = obtener_tickers('ACCIONES')

    for ticker in tickers_accion:
        cot_accion_hist(ticker)
        cot_accion_hist_bd(ticker)
    
    tickers_cedear = obtener_tickers('CEDEARS')

    cot_cedear_hist_bd(cot_cedear_hist(tickers_cedear))

    