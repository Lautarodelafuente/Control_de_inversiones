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

print(KEY)

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


if __name__ == '__main__':
    dolar = dolar_historico_bd()

    #print(dolar)
    #leer(rango_hoja='CCL!A2:C10')
    #borrar(rango_hoja='CCL!A2:C')
    #escribir(rango_hoja='CCL!A1:C10',valores=values)
    update(rango_hoja='CCL!A2',valores=dolar)




