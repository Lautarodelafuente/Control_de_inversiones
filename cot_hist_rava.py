import os
import csv
import logging
import requests
import bd_postgresql as bd

logger = logging.getLogger(__name__)

CARPETA_DESCARGAS = 'descargas_historicos'
TICKERS = ['SPY_US', 'QQQ_US']  # agregar acá el resto de ETFs del portfolio

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0 Safari/537.36'
}


def descargar_historico(ticker, carpeta=CARPETA_DESCARGAS):
    print(f'Descargando historico de {ticker}...')
    os.makedirs(carpeta, exist_ok=True)
    url = f'https://www.rava.com/perfil/descarga-historicos/{ticker}'
    ruta_archivo = os.path.join(carpeta, f'{ticker}.csv')

    try:
        respuesta = requests.get(url, headers=HEADERS, timeout=30)
        respuesta.raise_for_status()
    except requests.RequestException as e:
        logger.error(f'Error al descargar el historico de {ticker}: {e}')
        return None

    # Chequeo basico de que la respuesta sea realmente el CSV esperado
    if 'especie' not in respuesta.text[:200].lower():
        logger.error(f'La respuesta para {ticker} no parece ser el CSV esperado (¿pagina de error/login?).')
        return None

    with open(ruta_archivo, 'wb') as f:
        f.write(respuesta.content)

    logger.info(f'Historico de {ticker} descargado en {ruta_archivo}')
    return ruta_archivo


def leer_historico_csv(ruta_archivo):
    registros = []
    with open(ruta_archivo, encoding='utf-8-sig') as f:  # utf-8-sig saca el BOM
        lector = csv.DictReader(f)
        for fila in lector:
            try:
                registro = (
                    fila['especie'],
                    fila['fecha'],
                    float(fila['apertura']),
                    float(fila['maximo']),
                    float(fila['minimo']),
                    float(fila['cierre']),
                    float(fila['volumen']),
                    float(fila['usd_apertura']),
                    float(fila['usd_maximo']),
                    float(fila['usd_minimo']),
                    float(fila['usd_cierre']),
                )
                registros.append(registro)
            except (ValueError, KeyError) as e:
                logger.error(f'Fila con datos invalidos, se omite: {fila} ({e})')
                continue
    return registros


historico_upsert_query = """
    INSERT INTO public.rava_cotizacion_historica
        (ticker, fecha, apertura, maximo, minimo, cierre, volumen,
        usd_apertura, usd_maximo, usd_minimo, usd_cierre)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker, fecha)
    DO UPDATE SET
        apertura = EXCLUDED.apertura
        , maximo = EXCLUDED.maximo
        , minimo = EXCLUDED.minimo
        , cierre = EXCLUDED.cierre
        , volumen = EXCLUDED.volumen
        , usd_apertura = EXCLUDED.usd_apertura
        , usd_maximo = EXCLUDED.usd_maximo
        , usd_minimo = EXCLUDED.usd_minimo
        , usd_cierre = EXCLUDED.usd_cierre
    ;
"""


def cargar_historico_bd(registros, query=historico_upsert_query):
    if not registros:
        logger.warning('No hay registros para cargar.')
        return

    try:
        with bd.conexion_base_de_datos() as conn:
            with conn.cursor() as cur:
                conteo = 0
                for data in registros:
                    try:
                        cur.execute("SAVEPOINT sp_historico")
                        cur.execute(query, data)
                        cur.execute("RELEASE SAVEPOINT sp_historico")
                        conteo += 1
                    except Exception as e:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_historico")
                        logger.error(f'Error al insertar el registro {data}: {e}')
                        continue

                conn.commit()
                logger.info(f'Se insertaron/actualizaron {conteo} registros historicos.')

        conn.close()

    except Exception as e:
        logger.error(f'Error en la conexion o transaccion: {e}')


def procesar_historico(ticker, carpeta=CARPETA_DESCARGAS):
    ruta_archivo = descargar_historico(ticker, carpeta)
    if ruta_archivo is None:
        return

    registros = leer_historico_csv(ruta_archivo)
    cargar_historico_bd(registros)

    try:
        os.remove(ruta_archivo)
        logger.info(f'Archivo {ruta_archivo} eliminado.')
    except OSError as e:
        logger.error(f'No se pudo eliminar el archivo {ruta_archivo}: {e}')


def proceso_general_historico(tickers=TICKERS):
    for ticker in tickers:
        procesar_historico(ticker)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    proceso_general_historico()
    