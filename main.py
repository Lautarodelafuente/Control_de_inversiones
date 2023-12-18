import API_IOL as a_iol
import Base_de_datos_postgresql as bd
import DolarCCL_historico as CCL_hist
import DolarCCL_API as CCL_api
import Python_google_sheet

#--------------------------------------------------------------------------------#
# CONEXION A LA BASE DE DATOS
conn=bd.conexion_base_de_datos()

#--------------------------------------------------------------------------------#
# PROCESOS DOLARCCL

# Iniciamos el proceso pasandole el link de la url que queremos scrapear sobre dolarCCL
CCL_hist.proceso_gral('https://www.ambito.com/contenidos/dolar-cl-historico.html')

# Funcion que trae el dato del dolarCCL actual
CCL_api.insert_dolarCCL_api()


#--------------------------------------------------------------------------------#
# PROCESOS IOL

# Envia a la base de datos la informacion de operciones historicas de IOL
a_iol.operaciones_historicas_to_database(operaciones_historicas=a_iol.operaciones(),conn=conn)

# Envia a la base de datos la informacion de portafolio actual de IOL
a_iol.portafolio_actual_to_database(df_portafolio_actual=a_iol.portafolio(),conn=conn)


#--------------------------------------------------------------------------------#
# PROCESOS GOOGLE SHEETS

# Cargamos los datos del dolar en la planilla de google sheets
Python_google_sheet.update(rango_hoja='CCL!A2',valores=Python_google_sheet.dolar_historico_bd())


