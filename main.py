import API_IOL as a_iol
import Base_de_datos_postgresql as bd
import DolarCCL_historico as CCL_hist
import DolarCCL_API as CCL_api
import Python_google_sheet
import Cotizaciones_rava as CR
import delete_files
from datetime import datetime
from os.path import exists 
from os import system
import os 
from dotenv import load_dotenv



# Cargamos las variables de entorno del archivo .env
load_dotenv()

#--------------------------------------------------------------------------------#
# SETEAMOS EL LOG

inicio = datetime.now()
log_str = inicio.strftime('%Y%m%d_%H%M%S')
archivo_log = f'log_control_inversiones_{log_str}.log'
path_logs = os.getenv('Carpeta_Logs')

if not exists(path_logs):
    system('mkdir logs')

with open(f'{path_logs}/{archivo_log}','w') as log:

    log.write(f'{datetime.now()}: Iniciando ejecucion del programa...\n')

    #--------------------------------------------------------------------------------#
    # CONEXION A LA BASE DE DATOS
    try:
        conn=bd.conexion_base_de_datos()
        log.write(f'{datetime.now()}: EXITO! Conexion exitosa a la base de datos!\n')
    except Exception as e:
        log.write(f'{datetime.now()}: ERROR! Al tratar de conectarse a la base de datos: {e}\n')

    #--------------------------------------------------------------------------------#
    # CONEXION A LA BASE DE DATOS
    try:
        folder_path = "C:/Users/USER/.wdm/drivers/chromedriver/win64"
        delete_files.delite_file_and_folders(folder_path)
        log.write(f'{datetime.now()}: EXITO! Conexion exitosa a la base de datos!\n')
    except Exception as e:
        log.write(f'{datetime.now()}: ERROR! Al tratar de conectarse a la base de datos: {e}\n')

    #--------------------------------------------------------------------------------#
    # PROCESOS DOLARCCL

    # Iniciamos el proceso pasandole el link de la url que queremos scrapear sobre dolarCCL
    try:
        CCL_hist.proceso_gral('https://www.ambito.com/contenidos/dolar-cl-historico.html')
        log.write(f'{datetime.now()}: EXITO! Carga de datos DolarCCL historico exitosa!\n')
    except Exception as e:
        log.write(f'{datetime.now()}: ERROR! Al tratar de cargar los datos de DolarCCL historico: {e}\n')


    # Funcion que trae el dato del dolarCCL actual
    try:
        CCL_api.insert_dolarCCL_api()
        log.write(f'{datetime.now()}: EXITO! Carga de datos DolarCCL API diario exitosa!\n')
    except Exception as e:
        log.write(f'{datetime.now()}: ERROR! Al tratar de cargar los datos de DolarCCL API diario: {e}\n')

    #--------------------------------------------------------------------------------#
    # PROCESOS IOL

    # Envia a la base de datos la informacion de operciones historicas de IOL
    try:
        a_iol.operaciones_historicas_to_database(operaciones_historicas=a_iol.operaciones(),conn=conn)
        log.write(f'{datetime.now()}: EXITO! Carga de datos Operaciones IOL historicas exitosa!\n')
    except Exception as e:
        log.write(f'{datetime.now()}: ERROR! Al tratar de cargar los datos de Operaciones IOL historicas: {e}\n')
    
    
    # Envia a la base de datos la informacion de portafolio actual de IOL
    try:
        a_iol.portafolio_actual_to_database(df_portafolio_actual=a_iol.portafolio(),conn=conn)
        log.write(f'{datetime.now()}: EXITO! Carga de datos Portafolio IOL exitosa!\n')
    except Exception as e:
        log.write(f'{datetime.now()}: ERROR! Al tratar de cargar los datos de Portafolio IOL: {e}\n')


    #--------------------------------------------------------------------------------#
    # PROCESOS COTIZACION RAVA

    # Envia a la base de datos la informacion de Cotizaciones rava bursatil
    try:
        CR.proceso_general_rava()
        log.write(f'{datetime.now()}: EXITO! Carga de datos Cotizaciones rava exitosa!\n')
    except Exception as e:
        log.write(f'{datetime.now()}: ERROR! Al tratar de cargar los datos de Cotizaciones rava: {e}\n')
    
    
    #--------------------------------------------------------------------------------#
    # PROCESOS GOOGLE SHEETS

    # Cargamos los datos del dolar en la planilla de google sheets
    try:
        Python_google_sheet.update(rango_hoja='CCL!A2',valores=Python_google_sheet.dolar_historico_bd())
        log.write(f'{datetime.now()}: EXITO! Carga de datos DolarCCL en Google Sheets exitosa!\n')
    except Exception as e:
        log.write(f'{datetime.now()}: ERROR! Al tratar de cargar los datos de DolarCCL en Google Sheets: {e}\n')

    # Cargamos los datos de cotizaciones en la planilla de google sheets
    try:
        Python_google_sheet.update(rango_hoja='Cotizacion acciones Rava!A2',valores=Python_google_sheet.cotizacion_rava_bd())
        log.write(f'{datetime.now()}: EXITO! Carga de cotizaciones rava en Google Sheets exitosa!\n')
    except Exception as e:
        log.write(f'{datetime.now()}: ERROR! Al tratar de cargar los datos de cotizaciones rava en Google Sheets: {e}\n')


    final = datetime.now()
    delta = final - inicio

    log.write(f'{datetime.now()}: Tiempo de ejecucion: {delta}\n')
    log.write(f'{datetime.now()}: Trabajo finalizado\n')        

