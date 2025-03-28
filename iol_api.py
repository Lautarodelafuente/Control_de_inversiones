import os
import logging
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from datetime import date
import bd_postgresql as bd

# Elementos para la solicitud del token
'''
https://api.invertironline.com/token
POST /token HTTP/1.1
Host: api.invertironline.com
Content-Type: application/x-www-form-urlencoded
username=MIUSUARIO&password=MICONTRASEÑA&grant_type=password
'''

logger = logging.getLogger(__name__)

# Cargamos las variables de entorno
load_dotenv()

# URL base para la creacion de las apis de IOL
url_api = 'https://api.invertironline.com'


def obtener_token(url_base=url_api):

    logging.info('Obteniendo token iol...')

    # Encabezado necesario para la API del TOKEN
    header = {'Content-Type': 'application/x-www-form-urlencoded'}

    # creamos las variables de body necesarias para pasarle a la API del TOKEN
    body = {
        'username':os.getenv('USER'),
        'password':os.getenv('PASS'),
        'grant_type':'password'
    }

    try:
        response = requests.post(f'{url_base}/token', headers=header, data=body)
        response.raise_for_status()
        return response.json()["access_token"]
    except requests.RequestException as e:
        logging.exception("Error al obtener el token: %s", e)
        return None


def header_authorization_bearer(token):
    return {'Authorization': f'Bearer {token}'}


def operaciones(url_base=url_api):

    logging.info('Iniciando extracción de información de operaciones IOL...') 

    select_ultima_fecha_operaciones_historicas = '''select max(fechaoperada)::date from iol_operaciones_historicas;'''

    with bd.conexion_base_de_datos() as conn, conn.cursor() as cur:
        cur.execute(select_ultima_fecha_operaciones_historicas)
        fecha_desde = cur.fetchone()[0] or '2018-01-01'

    fecha_hasta = date.today()
    token = obtener_token()
    if not token:
        return []

    headers = header_authorization_bearer(token)
    params = {
        'filtro.estado':'todas', # OPCIONAL: todas, pendientes, canceladas, terminadas
        'filtro.fechaDesde':fecha_desde,
        'filtro.fechaHasta':fecha_hasta,
        'filtro.pais':'argentina' # OPCIONAL: estados_Unidos, argentina
    }

    try:
        response = requests.get(f'{url_base}/api/v2/operaciones', headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.exception("Error al obtener operaciones: %s", e)
        return []


def operaciones_historicas_to_database(operaciones_historicas,conn):

    logging.info('Migrando operaciones históricas a la BD...')

    # Si no existe creamos la tabla en la bd
    query_create = '''
    create table if not exists public.iol_operaciones_historicas (
        numero int primary key,
        fechaOrden timestamp not null,
        tipo varchar(100),
        estado varchar(100),
        mercado varchar(40),
        simbolo varchar(30),
        cantidad numeric(20,2),
        monto numeric(20,2),
        modalidad varchar(50),
        precio numeric(20,2),
        fechaOperada timestamp,
        cantidadOperada numeric(20,2),
        precioOperado numeric(20,2),
        montoOperado numeric(20,2),
        plazo varchar(30)
    );'''

    # Creamos la sentencia SQL
    query_insert = """
        INSERT INTO public.iol_operaciones_historicas
            (numero, fechaOrden, tipo, estado, mercado, simbolo, cantidad, monto, modalidad, precio, fechaOperada, cantidadOperada, precioOperado, montoOperado, plazo)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (numero) DO NOTHING;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(query_create)
            if not operaciones_historicas:
                logging.info("No hay nuevas operaciones para insertar.")
                return
            df = pd.DataFrame(operaciones_historicas)
            df.replace({np.nan: None})
            cur.executemany(query_insert, [tuple(x) for x in df.to_numpy()])
            conn.commit()
            logging.info(f'Se insertaron {len(df)} registros exitosamente.')
    except Exception as e:
        logging.exception("Error al migrar operaciones: %s", e)
        conn.rollback()


def portafolio(url_base=url_api):
    
    logging.info('Iniciando extracción de información de portafolio IOL...')
    
    token = obtener_token()
    if not token:
        return pd.DataFrame()
    
    headers = header_authorization_bearer(token)
    endpoint = f'{url_base}/api/v2/portafolio/argentina'
    
    try:
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        activos = response.json().get('activos', [])
        
        df = pd.DataFrame(activos)
        if df.empty:
            return df
        
        df_titulo = pd.json_normalize(df['titulo'])
        df = df.drop(columns=['titulo']).join(df_titulo)
        
        return df[['simbolo', 'descripcion', 'pais', 'mercado', 'tipo', 'plazo', 'moneda', 'cantidad', 'comprometido', 'puntosVariacion', 'variacionDiaria', 'ultimoPrecio', 'ppc', 'gananciaPorcentaje', 'gananciaDinero', 'valorizado', 'parking']]
    except requests.RequestException as e:
        logging.exception("Error al obtener portafolio: %s", e)
        return pd.DataFrame()


def portafolio_actual_to_database(df_portafolio_actual,conn):

    logging.info('Migrando portafolio actual a la BD...')
    
    # Si no existe creamos la tabla en la bd
    query_create = '''
    create table if not exists iol_portafolio_actual (
        id_portafolio serial primary key,
        simbolo varchar(15),
        descripcion varchar(100),
        pais varchar(50),
        mercado varchar(35),
        tipo varchar(50),
        plazo varchar(10),
        moneda varchar(50),
        cantidad numeric(10,2),
        comprometido numeric(10,2),
        puntosVariacion numeric(10,2),
        variacionDiaria numeric(10,2),
        ultimoPrecio numeric(20,2),
        ppc numeric(20,2),
        gananciaPorcentaje numeric(20,2),
        gananciaDinero numeric(20,2),
        valorizado numeric(20,2),
        parking varchar(50),
        fecha_actualizacion timestamp default now()
    );'''

    # Borrar datos
    delete_query = '''delete from public.iol_portafolio_actual;'''

    # Creamos la sentencia SQL
    query_insert = """
        INSERT INTO public.iol_portafolio_actual (simbolo, descripcion, pais, mercado, tipo, plazo, moneda, cantidad, comprometido, puntosvariacion, variaciondiaria, ultimoprecio, ppc, gananciaporcentaje, gananciadinero, valorizado, parking)
           values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """

    try:
        with conn.cursor() as cur:
            cur.execute(query_create)
            cur.execute(delete_query)
            df_portafolio_actual = df_portafolio_actual.replace({np.nan: None})
            cur.executemany(query_insert, [tuple(x) for x in df_portafolio_actual.to_numpy()])
            conn.commit()
            logging.info("Migración de portafolio exitosa.")
    except Exception as e:
        logging.exception("Error en migración de portafolio: %s", e)
        conn.rollback()


def Info_acciones_anual(url_base=url_api):

    print('Iniciando extraccion de informacion de portafolio IOL...')
    print()

    select_simbolos ='''
    select 
        distinct simbolo 
    from iol_portafolio_actual ipa
    where 
        ipa.tipo = 'CEDEARS' and 
        ipa.mercado = 'bcba' 
         and ipa.simbolo = 'SPY' ;
    '''
    
    conn = bd.conexion_base_de_datos()

    cur = conn.cursor()

    cur.execute(select_simbolos)

    simbolos = cur.fetchall()

    tickers = []
    Acciones_historicas = []

    for ticker in simbolos:
        tickers.append(ticker[0])

        simbolo = ticker[0]

        Select_fechas = ''' 
            select 
                min(ioh.fechaorden)::date as fechaDesde
            from iol_portafolio_actual ipa
                join iol_operaciones_historicas ioh on ipa.simbolo = ioh.simbolo 
            where 
                ioh.estado ilike '%terminada%' and 
                ipa.simbolo = '{simbolo}';
        '''

    '''
        cur.execute(Select_fechas)

        fechaDesde = date.today()
        fechaDesde = fechaDesde.replace(day = fechaDesde.day - 2)

        print(str(fechaDesde))
        fechaHasta = date.today()
        ajustada = 'sinAjustar'
        mercado = 'nYSE'

        endpoint = url_base + f'/api/v2/{mercado}/Titulos/{simbolo}/Cotizacion/seriehistorica/{fechaDesde}/{fechaHasta}/{ajustada}'
        print(endpoint)
        header = header_authorization_bearer()
        
        historico_accion = requests.get(endpoint, headers=header).json()

        #print(type(historico_accion))

        #historico_accion.append(simbolo)

        #print(historico_accion)
        df = pd.DataFrame(historico_accion)

        df["simbolo"] = simbolo

        print(df)

        
    #print(tickers)

    #print(Acciones_historicas)

    #df = pd.DataFrame(historico_accion)

    #print(df)
    '''
    '''mercado = 'bCBA'
    simbolo = 'spy'
    fechaDesde =  
    fechaHasta = 
    ajustada = 'ajustada'

    endpoint = url_base + f'/api/v2/{mercado}/Titulos/{simbolo}/Cotizacion/seriehistorica/{fechaDesde}/{fechaHasta}/{ajustada}'
    header = header_authorization_bearer()
    
    portafolio_actual = requests.get(endpoint, headers=header).json()'''
    

if __name__ == '__main__':

    #--------------------------------------------------------------------------------#
    # CONFIGURACIÓN DE LOGGING
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()] 
    )

    logging.info('Iniciando proceso...')
    conn = bd.conexion_base_de_datos()
    operaciones_historicas_to_database(operaciones(), conn)
    portafolio_actual_to_database(portafolio(), conn)
    logging.info('Proceso finalizado.')
