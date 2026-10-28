import os
import logging
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
import bd_postgresql as bd
from datetime import date, datetime, timedelta

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
def leer(rango_hoja):

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


HOJA_CCL = 'CCL'
RANGO_CCL = f'{HOJA_CCL}!A2:B'
EPOCH_SHEETS = date(1899, 12, 30)  # Google Sheets guarda las fechas como dias desde esta fecha

# LEE VALORES SIN FORMATO (fechas como numero de serie, precios como numero).
# A diferencia de leer(), devuelve None si falla, para no confundir un error con una hoja vacia.
def leer_crudo(rango_hoja):
    try:
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=rango_hoja,
            valueRenderOption='UNFORMATTED_VALUE',
            dateTimeRenderOption='SERIAL_NUMBER'
        ).execute()
        return result.get('values', [])
    except Exception as e:
        logger.exception('Error al leer datos: %s', e)
        return None


# ACTUALIZA VARIAS CELDAS SUELTAS EN UNA SOLA LLAMADA A LA API
def actualizar_celdas(datos):
    # datos: lista de (rango, valor), ej: [('CCL!B5', 1602.73), ('CCL!B9', 1590.5)]
    if not datos:
        return
    try:
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': [{'range': rango, 'values': [[valor]]} for rango, valor in datos]
        }
        sheet.values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
        logger.info(f'{len(datos)} celdas actualizadas correctamente.')
    except Exception as e:
        logger.exception('Error al actualizar celdas: %s', e)

# SI LA HOJA TIENE MENOS FILAS QUE LAS NECESARIAS, AGREGA LAS QUE FALTAN AL FINAL
def asegurar_filas(nombre_hoja, filas_necesarias):
    try:
        meta = sheet.get(
            spreadsheetId=SPREADSHEET_ID,
            fields='sheets(properties(sheetId,title,gridProperties(rowCount)))'
        ).execute()

        for s in meta['sheets']:
            props = s['properties']
            if props['title'] == nombre_hoja:
                faltan = filas_necesarias - props['gridProperties']['rowCount']
                if faltan > 0:
                    sheet.batchUpdate(
                        spreadsheetId=SPREADSHEET_ID,
                        body={'requests': [{
                            'appendDimension': {
                                'sheetId': props['sheetId'],
                                'dimension': 'ROWS',
                                'length': faltan
                            }
                        }]}
                    ).execute()
                    logger.info(f'Se agregaron {faltan} filas a la hoja {nombre_hoja}.')
                return True

        logger.error(f'No se encontro la hoja {nombre_hoja}.')
        return False
    except Exception as e:
        logger.exception('Error al ampliar la hoja: %s', e)
        return False

def a_fecha(valor):
    if isinstance(valor, (int, float)):
        return EPOCH_SHEETS + timedelta(days=int(valor))
    if isinstance(valor, str):
        for formato in ('%Y-%m-%d', '%d/%m/%Y'):
            try:
                return datetime.strptime(valor.strip(), formato).date()
            except ValueError:
                pass
    return None


def a_precio(valor):
    if isinstance(valor, (int, float)):
        return round(float(valor), 2)
    if isinstance(valor, str):
        texto = valor.strip()
        if ',' in texto:
            texto = texto.replace('.', '').replace(',', '.')
        try:
            return round(float(texto), 2)
        except ValueError:
            return None
    return None


# CONVIERTE LO LEIDO DE LA HOJA EN {fecha: (nro_fila, precio)}
def parsear_hoja(valores):
    hoja = {}
    for i, fila in enumerate(valores):
        nro_fila = i + 2  # el rango arranca en la fila 2
        if not fila or fila[0] in ('', None):
            continue
        fecha = a_fecha(fila[0])
        if fecha is None:
            logger.warning(f'Fila {nro_fila}: fecha no reconocida ({fila[0]!r}), se ignora.')
            continue
        precio = a_precio(fila[1]) if len(fila) > 1 else None
        if fecha in hoja:
            logger.warning(f'Fila {nro_fila}: fecha {fecha} duplicada, se ignora.')
            continue
        hoja[fecha] = (nro_fila, precio)
    proxima_fila = len(valores) + 2  # primera fila libre debajo de la tabla
    return hoja, proxima_fila


# TRAE LA VISTA V_DOLAR_CCL_HISTORICO COMO {fecha: precio}
def ccl_desde_bd():
    datos = obtener_datos_bd('SELECT fecha, precio_dolar FROM v_dolar_ccl_historico;')
    return {
        fila[0]: round(float(fila[1]), 2)
        for fila in datos
        if fila[0] is not None and fila[1] is not None
    }


def calcular_diferencias(hoja, bd_ccl):
    faltantes, distintos = [], []
    for fecha, precio in bd_ccl.items():
        if fecha not in hoja:
            faltantes.append((fecha, precio))
            continue
        nro_fila, precio_hoja = hoja[fecha]
        if precio_hoja is None or abs(precio_hoja - precio) > 0.001:
            distintos.append((nro_fila, precio))
    faltantes.sort(key=lambda x: x[0], reverse=True)  # mas reciente primero, como la vista
    return faltantes, distintos


def sincronizar_ccl():
    logger.info('Sincronizando hoja CCL con la vista v_dolar_ccl_historico...')

    valores = leer_crudo(RANGO_CCL)
    if valores is None:
        logger.error('No se pudo leer la hoja CCL, se cancela la sincronizacion.')
        return
    hoja, proxima_fila = parsear_hoja(valores)

    bd_ccl = ccl_desde_bd()
    if not bd_ccl:
        logger.warning('La vista no devolvio datos, no hay nada para sincronizar.')
        return

    faltantes, distintos = calcular_diferencias(hoja, bd_ccl)
    logger.info(f'Fechas faltantes en la hoja: {len(faltantes)} | Precios distintos: {len(distintos)}')

    # Precios que cambiaron: se corrige solo la celda B de esa fila
    actualizar_celdas([(f'{HOJA_CCL}!B{nro}', precio) for nro, precio in distintos])

    # Fechas que no estaban: se agregan debajo de la ultima fila de la tabla
    if faltantes:
        ultima_fila_necesaria = proxima_fila + len(faltantes) - 1
        if asegurar_filas(HOJA_CCL, ultima_fila_necesaria):
            update(f'{HOJA_CCL}!A{proxima_fila}', [[f.isoformat(), p] for f, p in faltantes])

    # Fechas que estan en la hoja pero no en la vista: se cargan en la BD
    solo_en_hoja = sorted(
        ((f, p) for f, (nro_fila, p) in hoja.items() if f not in bd_ccl),
        key=lambda x: x[0]
    )
    a_insertar = [(f, p) for f, p in solo_en_hoja if p is not None and p > 0]
    sin_precio = [f for f, p in solo_en_hoja if p is None or p <= 0]

    if sin_precio:
        logger.warning(
            f'Fechas en la hoja sin precio valido, no se cargan en la BD: '
            f'{[f.isoformat() for f in sin_precio]}'
        )
    if a_insertar:
        logger.info(f'Fechas solo en la hoja, a cargar en la BD: {[f.isoformat() for f, _ in a_insertar[:10]]}')
        insertar_ccl_bd(a_insertar)

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

# CARGA EN dolar_ccl_historico LAS FECHAS QUE ESTAN EN LA HOJA Y NO EN LA BD
def insertar_ccl_bd(registros):
    # registros: lista de (fecha, precio)
    if not registros:
        return

    conn = bd.conexion_base_de_datos()
    if conn is None:
        logger.error('No se pudo conectar a la base de datos.')
        return

    query = """
        INSERT INTO public.dolar_ccl_historico (fecha, referencia, fecha_actualizacion)
        VALUES (%s, %s, NOW())
        ON CONFLICT (fecha) DO NOTHING;
    """
    try:
        with conn, conn.cursor() as cur:
            insertados = 0
            for data in registros:
                try:
                    cur.execute("SAVEPOINT sp_ccl")
                    cur.execute(query, data)
                    cur.execute("RELEASE SAVEPOINT sp_ccl")
                    insertados += cur.rowcount  # 1 si inserto, 0 si ya existia
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_ccl")
                    logger.error(f'Error al insertar el registro {data}: {e}')
        logger.info(f'{insertados} fechas de la hoja cargadas en dolar_ccl_historico.')
    except Exception as e:
        logger.exception('Error en la conexion o transaccion: %s', e)
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
    SELECT ticker, ultima_cotizacion, porcentaje_gan_dia, porcentaje_gan_mes, porcentaje_gan_anio,
           cotizacion_anterior, cotizacion_apertura, cotizacion_minimo, cotizacion_maximo, hora,
           vol_nominal, vol_efectivo, '0' as ratio, 0 as ccl, tipo_accion
      FROM public.rava_cotizacion_acciones_arg_lider
    UNION ALL
    SELECT ticker, ultima_cotizacion, porcentaje_gan_dia, porcentaje_gan_mes, porcentaje_gan_anio,
           cotizacion_anterior, cotizacion_apertura, cotizacion_minimo, cotizacion_maximo, hora,
           vol_nominal, vol_efectivo, '0' as ratio, 0 as ccl, tipo_accion
      FROM rava_cotizacion_acciones_arg_gral
    UNION ALL
    SELECT ticker, ultima_cotizacion, porcentaje_gan_dia, porcentaje_gan_mes, porcentaje_gan_anio,
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
    #update('CCL!A2', dolar_historico_bd())
    sincronizar_ccl()
    update('Cotizacion acciones Rava!A2', cotizacion_rava_bd())
    logging.info('Proceso finalizado.')

    




