import logging
import os
import sys
from dotenv import load_dotenv
from datetime import datetime

import iol_api as a_iol
import bd_postgresql as bd
import dolarccl_historico as CCL_hist
import dolarccl_api as CCL_api
import python_google_sheet
import cot_diaria_rava as CR
from Services import delete_files
from Services.scraping import iniciar_scraping

# Cargamos las variables de entorno del archivo .env
load_dotenv()

#--------------------------------------------------------------------------------#
# SETEAMOS EL LOG

inicio = datetime.now()
log_str = inicio.strftime('%Y%m%d_%H%M%S')
archivo_log = f'log_control_inversiones_{log_str}.log'
path_logs = os.getenv('Carpeta_Logs', 'logs')

# Crear carpeta de logs si no existe
os.makedirs(path_logs, exist_ok=True)

ruta_log = os.path.join(path_logs, archivo_log)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(ruta_log),
        logging.StreamHandler(sys.stdout)
    ]
)

logging.info("Iniciando ejecucion del programa...")

#--------------------------------------------------------------------------------#
# CONEXION A LA BASE DE DATOS
conn = None
try:
    conn = bd.conexion_base_de_datos()
    logging.info("EXITO! Conexion exitosa a la base de datos!")
except Exception as e:
    logging.error(f"ERROR! Al tratar de conectarse a la base de datos: {e}")

#--------------------------------------------------------------------------------#
# Limpieza de cache
logging.info('')
logging.info(f'INICIO - LIMPIEZA DE CACHE...')
try:
    folder_path = "C:/Users/USER/.wdm/drivers/chromedriver/win64"
    delete_files.delite_files_and_folders(folder_path)
    logging.info("EXITO! Cache del driver borrada!")
except Exception as e:
    logging.error(f"ERROR! Al tratar de borrar cache del driver: {e}")

#--------------------------------------------------------------------------------#
# PROCESOS DOLARCCL
logging.info('')
logging.info(f'INICIO - CARGA DOLARCCL HISTORICO...')
# Iniciamos el proceso pasandole el link de la url que queremos scrapear sobre dolarCCL
try:
    CCL_hist.proceso_gral('https://www.ambito.com/contenidos/dolar-cl-historico.html')
    logging.info("EXITO! Carga de datos DolarCCL historico exitosa!")
except Exception as e:
    logging.error(f"ERROR! Al tratar de cargar los datos de DolarCCL historico: {e}")

logging.info('')
logging.info(f'INICIO - CARGA DOLARCCL HOY...')
# Funcion que trae el dato del dolarCCL actual
try:
    CCL_api.insert_dolarCCL_api()
    logging.info("EXITO! Carga de datos DolarCCL API diaria exitosa!")
except Exception as e:
    logging.error(f"ERROR! Al tratar de cargar los datos de DolarCCL API diario: {e}")

#--------------------------------------------------------------------------------#
# PROCESOS IOL
logging.info('')
logging.info(f'INICIO - CARGA DATOS IOL...')
if conn:
    # Envia a la base de datos la informacion de operciones historicas de IOL
    try:
        a_iol.operaciones_historicas_to_database(operaciones_historicas=a_iol.operaciones(), conn=conn)
        logging.info("EXITO! Carga de datos de Operaciones IOL historicas exitosa!")
    except Exception as e:
        logging.error(f"ERROR! Al tratar de cargar los datos de Operaciones IOL historicas: {e}")

    # Envia a la base de datos la informacion de portafolio actual de IOL
    try:
        a_iol.portafolio_actual_to_database(df_portafolio_actual=a_iol.portafolio(), conn=conn)
        logging.info("EXITO! Carga de datos del Portafolio IOL exitosa!")
    except Exception as e:
        logging.error(f"ERROR! Al tratar de cargar los datos del Portafolio IOL: {e}")

#--------------------------------------------------------------------------------#
# PROCESOS COTIZACION RAVA
logging.info('')
logging.info(f'INICIO - CARGA COTIZACIONES RAVA...')
# Envia a la base de datos la informacion de Cotizaciones rava bursatil
try:
    CR.proceso_general_rava()
    logging.info("EXITO! Carga de datos de Cotizaciones Rava exitosa!")
except Exception as e:
    logging.error(f"ERROR! Al tratar de cargar los datos de Cotizaciones Rava: {e}")

#--------------------------------------------------------------------------------#
# PROCESOS GOOGLE SHEETS
logging.info('')
logging.info(f'INICIO - CARGA DATOS EN GOOGLE SHEETS...')
# Cargamos los datos del dolar en la planilla de google sheets
try:
    python_google_sheet.update(rango_hoja='CCL!A2', valores=python_google_sheet.dolar_historico_bd())
    logging.info("EXITO! Carga de datos de DolarCCL en Google Sheets exitosa!")
except Exception as e:
    logging.error(f"ERROR! Al tratar de cargar los datos de DolarCCL en Google Sheets: {e}")

try:
    python_google_sheet.update(rango_hoja='Cotizacion acciones Rava!A2', valores=python_google_sheet.cotizacion_rava_bd())
    logging.info("EXITO! Carga de cotizaciones Rava en Google Sheets exitosa!")
except Exception as e:
    logging.error(f"ERROR! Al tratar de cargar los datos de cotizaciones Rava en Google Sheets: {e}")

# --------------------------------------------------------------------------------#
# CERRAMOS LA CONEXIoN A LA BASE DE DATOS
if conn:
    try:
        conn.close()
        logging.info(f'EXITO! Conexion a la base de datos cerrada correctamente.')
    except Exception as e:
        logging.error(f"ERROR! Al cerrar la conexion a la base de datos: {e}")

# --------------------------------------------------------------------------------#
# TIEMPO TOTAL DE EJECUCIoN
delta = datetime.now() - inicio
logging.info('')
logging.info(f"Tiempo de ejecucion: {delta}")
logging.info("Trabajo finalizado")    

