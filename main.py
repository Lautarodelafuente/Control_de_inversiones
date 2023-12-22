import API_IOL as a_iol
import Base_de_datos_postgresql as bd
import DolarCCL_historico as CCL_hist
import DolarCCL_API as CCL_api
import Python_google_sheet
from datetime import datetime
from os.path import exists 
from os import system

#--------------------------------------------------------------------------------#
# SETEAMOS EL LOG

inicio = datetime.now()
log_str = inicio.strftime('%Y%m%d_%H%M%S')
archivo_log = f'log_control_inversiones_{log_str}.log'

if not exists('Control_de_inversiones/logs'):
    system('mkdir logs')

with open(f'Control_de_inversiones/logs/{archivo_log}','w') as log:

    log.write(f'{datetime.now()}: Iniciando ejecucion del programa...\n')

    #--------------------------------------------------------------------------------#
    # CONEXION A LA BASE DE DATOS
    try:
        conn=bd.conexion_base_de_datos()
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
    # PROCESOS GOOGLE SHEETS

    # Cargamos los datos del dolar en la planilla de google sheets
    try:
        Python_google_sheet.update(rango_hoja='CCL!A2',valores=Python_google_sheet.dolar_historico_bd())
        log.write(f'{datetime.now()}: EXITO! Carga de datos DolarCCL en Google Sheets exitosa!\n')
    except Exception as e:
        log.write(f'{datetime.now()}: ERROR! Al tratar de cargar los datos de DolarCCL en Google Sheets: {e}\n')


    final = datetime.now()
    delta = final - inicio

    log.write(f'{datetime.now()}: Tiempo de ejecucion: {delta}\n')
    log.write(f'{datetime.now()}: Trabajo finalizado\n')        

