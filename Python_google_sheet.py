from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import Base_de_datos_postgresql as bd
import pandas as pd
import os
from dotenv import load_dotenv


# Cargamos las variables de entorno del archivo .env
load_dotenv()

# SETEAMOS LOS PARAMETROS DE LA API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
KEY = os.getenv('PATH_GOOGLE') + '\SERVICE_ACOUNT_KEY.json'

#print(KEY)

# Escribe aquí el ID de tu documento:
SPREADSHEET_ID = os.getenv('MIS_INVERSIONES')

# Enviamos las credenciales
creds = None
creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

# Activamos el servicio
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()


# LEER EN EL GOOGLE SHEET
def leer (rango_hoja):

    # Llamada a la api
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=rango_hoja).execute()
    # Extraemos values del resultado
    values = result.get('values',[])
    print(values)


# BORRAR DATOS DE LA HOJA
def borrar(rango_hoja):

    sheet.values().clear(spreadsheetId=SPREADSHEET_ID,range=rango_hoja).execute()

    print(f'valores del rango: {rango_hoja} fueron eliminados!')


# ESCRIBIR EN EL GOOGLE SHEET
def escribir(rango_hoja, valores):

    try:
        # Llamamos a la api
        result = sheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=rango_hoja,
            valueInputOption='USER_ENTERED',
            body={'values':valores}
        ).execute()

        print(f"Datos insertados correctamente.\n{(result.get('updates').get('updatedCells'))}")

    except Exception as e:
        print(f'Error al updetear los registros en {rango_hoja}:\n      {e}')
    
    
# BORRA LOS REGISTROS Y CARGA LOS NUEVOS EN EL GOOGLE SHEET
def update(rango_hoja, valores):
    
    try:
        # Llamamos a la api
        result = sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=rango_hoja,
            valueInputOption='USER_ENTERED',
            body={'values':valores}
        ).execute()

    except Exception as e:
        print(f'Error al updetear los registros en {rango_hoja}:\n      {e}')

    return result


# TRAE LOS DATOS DE LA VISTA V_DOLAR_CCL_HISTORICO DE LA BASE DE DATOS
def dolar_historico_bd():

    dolar_historico_bd = '''select * from v_dolar_ccl_historico;'''

    conn = bd.conexion_base_de_datos()

    cur = conn.cursor()

    cur.execute(dolar_historico_bd)

    df_dolar_historico = cur.fetchall()

    dolar = []

    for i in range(len(df_dolar_historico)):
        dolar.append([str(df_dolar_historico[i][0]),str(df_dolar_historico[i][1]).replace('.',',')])

    return dolar


# TRAE LOS DATOS DE LA VISTA V_DOLAR_CCL_HISTORICO DE LA BASE DE DATOS
def cotizacion_rava_bd():

    cotizacion_rava_bd = '''
    SELECT ticker, ultima_cotizacion, porcentaje_gan_dia, porcentaje_gan_mes, porcentaje_gan_año, cotizacion_anterior, cotizacion_apertura, cotizacion_minimo, cotizacion_maximo, hora, vol_nominal, vol_efectivo,'0' as ratio,0 as ccl, tipo_accion
        FROM public.rava_cotizacion_acciones_arg_lider
    union all
    select ticker, ultima_cotizacion, porcentaje_gan_dia, porcentaje_gan_mes, porcentaje_gan_año, cotizacion_anterior, cotizacion_apertura, cotizacion_minimo, cotizacion_maximo, hora, vol_nominal, vol_efectivo,'0' as ratio,0 as ccl, tipo_accion 
        from rava_cotizacion_acciones_arg_gral 
    union all
    select ticker, ultima_cotizacion, porcentaje_gan_dia, porcentaje_gan_mes, porcentaje_gan_año, cotizacion_anterior, cotizacion_apertura, cotizacion_minimo, cotizacion_maximo, hora, vol_nominal, vol_efectivo,ratio,ccl, 'Cedear' as tipo_accion 
        from rava_cotizacion_cedear_diaria 
    '''

    conn = bd.conexion_base_de_datos()

    cur = conn.cursor()

    cur.execute(cotizacion_rava_bd)

    df_cotizacion_rava_bd = cur.fetchall()

    #print(df_cotizacion_cedear_bd[1][0])

    cotizacion_rava = []

    filas = []

    for i in range(len(df_cotizacion_rava_bd)):
        #print(df_cotizacion_cedear_bd[i])
        for e in range(len(df_cotizacion_rava_bd[i])):
            #df_cotizacion_cedear_bd[i][e] = str(df_cotizacion_cedear_bd[i][e]).replace('.',',')
            filas.append(str(df_cotizacion_rava_bd[i][e]).replace('.',','))
        cotizacion_rava.append(filas)
        filas = []
    
    #print(cedear_rava)
        
    return cotizacion_rava


if __name__ == '__main__':
    
    dolar = dolar_historico_bd()
    cotizacion_rava = cotizacion_rava_bd()
    update(rango_hoja='CCL!A2',valores=dolar)
    update(rango_hoja='Cotizacion Cedear Rava!A2',valores=cotizacion_rava)

    #print(cotizacion_cedear_rava)

    #leer(rango_hoja='CCL!A2:C10')
    #borrar(rango_hoja='CCL!A2:C')
    #escribir(rango_hoja='CCL!A1:C10',valores=values)
    




