import os
import logging
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
import bd_postgresql as bd

logger = logging.getLogger(__name__)

# Cargamos las variables de entorno del archivo .env
load_dotenv()

# SETEAMOS LOS PARAMETROS DE LA API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
KEY = os.getenv('PATH_GOOGLE') + '\SERVICE_ACOUNT_KEY.json'
# Escribe aquí el ID de tu documento:
SPREADSHEET_ID = os.getenv('MIS_INVERSIONES')

logger.info('Configurando credenciales de Google Sheets...')

# Enviamos las credenciales
try:
    creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    logger.info('Credenciales configuradas correctamente.')
except Exception as e:
    logger.exception('Error al configurar credenciales de Google Sheets: %s', e)


# LEER EN EL GOOGLE SHEET
def leer (rango_hoja):

    try:
        logger.info(f'Leyendo datos de la hoja: {rango_hoja}')
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=rango_hoja).execute()
        values = result.get('values', [])
        return values
    except Exception as e:
        logger.exception('Error al leer datos: %s', e)
        return []


# BORRAR DATOS DE LA HOJA
def borrar(rango_hoja):

    try:
        logger.info(f'Borrando datos en el rango: {rango_hoja}')
        sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range=rango_hoja).execute()
        logger.info('Datos eliminados correctamente.')
    except Exception as e:
        logger.exception('Error al borrar datos: %s', e)


# ESCRIBIR EN EL GOOGLE SHEET
def escribir(rango_hoja, valores):

    try:
        logger.info(f'Escribiendo datos en {rango_hoja}')
        result = sheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=rango_hoja,
            valueInputOption='USER_ENTERED',
            body={'values': valores}
        ).execute()
        logger.info(f"Datos insertados correctamente en {rango_hoja}: {result.get('updates').get('updatedCells')} celdas actualizadas.")
    except Exception as e:
        logger.exception('Error al escribir datos: %s', e)
    
# BORRA LOS REGISTROS Y CARGA LOS NUEVOS EN EL GOOGLE SHEET
def update(rango_hoja, valores):
    
    try:
        logger.info(f'Actualizando datos en {rango_hoja}')
        result = sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=rango_hoja,
            valueInputOption='USER_ENTERED',
            body={'values': valores}
        ).execute()
        logger.info(f'Datos actualizados correctamente en {rango_hoja}.')
        return result
    except Exception as e:
        logger.exception('Error al actualizar datos: %s', e)
        return None

# Funciones para obtener datos de la base de datos
def obtener_datos_bd(query):
    conn = bd.conexion_base_de_datos()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()
    except Exception as e:
        logger.exception('Error al obtener datos de la BD: %s', e)
        return []
    finally:
        conn.close()

# TRAE LOS DATOS DE LA VISTA V_DOLAR_CCL_HISTORICO DE LA BASE DE DATOS
def dolar_historico_bd():
    logger.info('Obteniendo datos históricos del dólar desde la BD...')
    query = 'SELECT * FROM v_dolar_ccl_historico;'
    datos = obtener_datos_bd(query)
    return [[str(fila[0]), str(fila[1]).replace('.', ',')] for fila in datos]


# TRAE LOS DATOS DE LA VISTA V_DOLAR_CCL_HISTORICO DE LA BASE DE DATOS
def cotizacion_rava_bd():
    logger.info('Obteniendo cotizaciones de Rava desde la BD...')
    query = '''
    SELECT ticker, ultima_cotizacion, porcentaje_gan_dia, porcentaje_gan_mes, porcentaje_gan_año,
           cotizacion_anterior, cotizacion_apertura, cotizacion_minimo, cotizacion_maximo, hora,
           vol_nominal, vol_efectivo, '0' as ratio, 0 as ccl, tipo_accion
      FROM public.rava_cotizacion_acciones_arg_lider
    UNION ALL
    SELECT ticker, ultima_cotizacion, porcentaje_gan_dia, porcentaje_gan_mes, porcentaje_gan_año,
           cotizacion_anterior, cotizacion_apertura, cotizacion_minimo, cotizacion_maximo, hora,
           vol_nominal, vol_efectivo, '0' as ratio, 0 as ccl, tipo_accion
      FROM rava_cotizacion_acciones_arg_gral
    UNION ALL
    SELECT ticker, ultima_cotizacion, porcentaje_gan_dia, porcentaje_gan_mes, porcentaje_gan_año,
           cotizacion_anterior, cotizacion_apertura, cotizacion_minimo, cotizacion_maximo, hora,
           vol_nominal, vol_efectivo, ratio, ccl, 'Cedear' as tipo_accion
      FROM rava_cotizacion_cedear_diaria;
    '''
    datos = obtener_datos_bd(query)
    return [[str(valor).replace('.', ',') for valor in fila] for fila in datos]


if __name__ == '__main__':
    #--------------------------------------------------------------------------------#
    # CONFIGURACIÓN DE LOGGING
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()] 
    )

    logging.info('Iniciando actualización de Google Sheets...')
    update('CCL!A2', dolar_historico_bd())
    update('Cotizacion Cedear Rava!A2', cotizacion_rava_bd())
    logging.info('Proceso finalizado.')

    




