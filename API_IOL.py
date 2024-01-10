import requests
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from datetime import date
from datetime import datetime
from datetime import timedelta
import Base_de_datos_postgresql as bd
import json

# Elementos para la solicitud del token
'''
https://api.invertironline.com/token
POST /token HTTP/1.1
Host: api.invertironline.com
Content-Type: application/x-www-form-urlencoded
username=MIUSUARIO&password=MICONTRASEÑA&grant_type=password
'''

# Cargamos las variables de entorno
load_dotenv()

# URL base para la creacion de las apis de IOL
URL_API = 'https://api.invertironline.com'


def obtener_token(URL_BASE=URL_API):

    print('Obteniendo token...')
    print()

    # Encabezado necesario para la API del TOKEN
    header = {'Content-Type': 'application/x-www-form-urlencoded'}

    # creamos las variables de body necesarias para pasarle a la API del TOKEN
    body = {
        'username':os.getenv('USER'),
        'password':os.getenv('PASS'),
        'grant_type':'password'
    }

    # Creamos la request para conseguir el objeto TOKEN donde recibiremos el token de acceso a la api, el token para actualizarlo en caso de ser necesaro, entre otros datos como la fecha de solicitud, tiempo de expiracion, ect. # 'access_token', 'token_type', 'expires_in' (segundos), 'refresh_token', '.issued' (fecha de solicitud), '.expires' (fecha de expiracion), '.refreshexpires'
    token = requests.post(URL_BASE + '/token', headers=header, data=body)

    print(f'Respuesta: {token}')
    print()

    object_token = token.json()

    access_tokens = object_token["access_token"]
    #expire_datetime = object_token[".expires"]

    print('Aceso al token concedido!')
    print()
    print(access_tokens)
    print()
    #print(f'datetime_expire = {expire_datetime}')
    #print(datetime.now() + timedelta(seconds=899))

    return access_tokens


def header_authorization_bearer(token=obtener_token()):

    header = {'Authorization': 'Bearer ' + token }

    print('Listo el header Bearer para la api')
    print()

    return header


def operaciones(URL_BASE=URL_API):

    print('Iniciando extraccion de informacion de operaciones IOL...')
    print() 

    select_ultima_fecha_operaciones_historicas = '''
        select max(fechaoperada)::date from iol_operaciones_historicas;
    '''

    conn = bd.conexion_base_de_datos()

    cur = conn.cursor()

    cur.execute(select_ultima_fecha_operaciones_historicas)

    ultima_fecha_operaciones_historicas = cur.fetchone()

    print(f'fecha_desde: {ultima_fecha_operaciones_historicas}')
    print(f'fecha_hasta: {date.today()}')
    print() 

    endpoint = URL_BASE + '/api/v2/operaciones'
    header = header_authorization_bearer()
    parameters = {
        'filtro.estado':'todas', # OPCIONAL: todas, pendientes, canceladas, terminadas
        'filtro.fechaDesde':ultima_fecha_operaciones_historicas,
        #'filtro.fechaDesde':'2018-01-01',
        'filtro.fechaHasta':date.today(),
        'filtro.pais':'argentina' # OPCIONAL: estados_Unidos, argentina
    }

    operaciones_historicas = requests.get(endpoint, headers=header, params=parameters).json()

    print('Operaciones IOL extraidas con exito')
    print() 

    return operaciones_historicas


def operaciones_historicas_to_database(operaciones_historicas,conn):

    print('Iniciando migracion a BD Postgresql...')
    print() 

    # Si no existe creamos la tabla en la bd
    iol_operaciones_historicas_tabla = '''
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
    upsert_query = """
        INSERT INTO public.iol_operaciones_historicas
            (numero, fechaOrden, tipo, estado, mercado, simbolo, cantidad, monto, modalidad, precio, fechaOperada, cantidadOperada, precioOperado, montoOperado, plazo)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (numero)
        DO NOTHING;
    """

    try:
        # Creamos el cursos para poder impactar la base de datos
        cur = conn.cursor()
        print('Conectado el cursor!')
        print()

        # Ejecutamos la creacion de la tabla
        cur.execute(iol_operaciones_historicas_tabla)

        df = pd.DataFrame(operaciones_historicas)

        df = df.replace({np.nan: None})

        print(df)

        tupla = [tuple(x) for x in df.to_numpy()]

        # Hacemos un bucle for para asociar cada fila con la sentencia sql
        for data in tupla:
            cur.execute(upsert_query, data)
            #print('Insert realizado con exito!')

        print(f'Se revisaron, insertaron o updetearon {len(tupla)} registros con exito')
        print()

        # Commit de los cambios
        conn.commit()
        print('Listo el commit!')
        print()

        # Cerramos el cursor
        cur.close()
        print('Cerramos el cursor')
        print()    

        print('Migracion realizada con exito!')
        print() 
        
            
    except Exception as e:
        print(f'Error al migrar los registros: {e}')  
        print()       

        # Commit de los cambios
        conn.rollback()
        print('Listo el rollback!')
        print()

        # Cerramos el cursor
        cur.close()
        print('Cerramos el cursor')
        print()


def portafolio(URL_BASE=URL_API):
    
    print('Iniciando extraccion de informacion de portafolio IOL...')
    print()

    endpoint = URL_BASE + '/api/v2/portafolio/argentina'
    header = header_authorization_bearer()
    
    portafolio_actual = requests.get(endpoint, headers=header).json()

    '''print('Portafolio:')
    print(portafolio_actual)'''

    activos_portafolio_arg = portafolio_actual['activos']

    portafolio = []

    for activo in activos_portafolio_arg:
        titulo = activo['titulo']
        activo['simbolo'] = titulo['simbolo']
        activo['descripcion'] = titulo['descripcion']
        activo['pais'] = titulo['pais']
        activo['mercado'] = titulo['mercado']
        activo['tipo'] = titulo['tipo']
        activo['plazo'] = titulo['plazo']
        activo['moneda'] = titulo['moneda']
        activo.pop('titulo')
        portafolio.append(activo)


    df = pd.DataFrame(portafolio)

    df_portafolio_actual = df[['simbolo', 'descripcion', 'pais', 'mercado','tipo', 'plazo', 'moneda','cantidad', 'comprometido', 'puntosVariacion', 'variacionDiaria','ultimoPrecio', 'ppc', 'gananciaPorcentaje', 'gananciaDinero','valorizado', 'parking']]

    print(df)

    print('Operaciones IOL extraidas con exito')
    print() 

    return df_portafolio_actual


def portafolio_actual_to_database(df_portafolio_actual,conn):

    print('Iniciando migracion a BD Postgresql...')
    print() 

    # Si no existe creamos la tabla en la bd
    iol_portafolio_actual_tabla = '''
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
    insert_query = """
        INSERT INTO public.iol_portafolio_actual (simbolo, descripcion, pais, mercado, tipo, plazo, moneda, cantidad, comprometido, puntosvariacion, variaciondiaria, ultimoprecio, ppc, gananciaporcentaje, gananciadinero, valorizado, parking)
           values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """

    try:
        # Creamos el cursos para poder impactar la base de datos
        cur = conn.cursor()
        print('Conectado el cursor!')
        print()

        df_portafolio_actual = df_portafolio_actual.replace({np.nan: None})

        print(df_portafolio_actual)

        tupla = [tuple(x) for x in df_portafolio_actual.to_numpy()]

        #print(tupla)

        # Ejecutamos la creacion de la tabla
        cur.execute(iol_portafolio_actual_tabla)

        # Ejecutamos el borrado de informacion de la tabla
        cur.execute(delete_query)

        # Hacemos un bucle for para asociar cada fila con la sentencia sql
        for data in tupla:
            cur.execute(insert_query, data)

        print(f'Se revisaron, insertaron o updetearon {len(tupla)} registros con exito')
        print()

        # Commit de los cambios
        conn.commit()
        print('Listo el commit!')
        print()

        # Cerramos el cursor
        cur.close()
        print('Cerramos el cursor')
        print()    

        print('Migracion realizada con exito!')
        print() 
        
            
    except Exception as e:
        print(f'Error al migrar los registros: {e}')  
        print()       

        # Commit de los cambios
        conn.rollback()
        print('Listo el rollback!')
        print()

        # Cerramos el cursor
        cur.close()
        print('Cerramos el cursor')
        print()


def Info_acciones_anual(URL_BASE=URL_API):

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

        endpoint = URL_BASE + f'/api/v2/{mercado}/Titulos/{simbolo}/Cotizacion/seriehistorica/{fechaDesde}/{fechaHasta}/{ajustada}'
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

    endpoint = URL_BASE + f'/api/v2/{mercado}/Titulos/{simbolo}/Cotizacion/seriehistorica/{fechaDesde}/{fechaHasta}/{ajustada}'
    header = header_authorization_bearer()
    
    portafolio_actual = requests.get(endpoint, headers=header).json()'''
    

if __name__ == '__main__':

    print('Iniciando proceso...')
    print() 

    conn=bd.conexion_base_de_datos()

    operaciones_historicas_to_database(operaciones_historicas=operaciones(),conn=conn)

    portafolio_actual_to_database(df_portafolio_actual=portafolio(),conn=conn)

    print('Proceso finalizado! Hasta la proxima!')
    print() 
