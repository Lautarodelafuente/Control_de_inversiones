from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import Select
import time
import Base_de_datos_postgresql as bd
import pandas as pd 
import psycopg2 as sql
from Services.scraping import iniciar_scraping


#-----------------------------------------------------------------------------------------------------------------------------------------------#

def obtener_cedears(driver, url='https://www.rava.com/cotizaciones/cedears'):

    # Con el driver previamente cargado, se abre chrome, y le pasamos la url que queremos que entre 
    driver.get(url)

    time.sleep(10)

    # Ubicamos el elemento tbody de la tabla de dolar historico con XPATH (camino html para ubicar el elemento). Busca solo el contenido, no las etiquetas
    elemento_tabla = driver.find_element(By.XPATH, '//table[@id="table"]')
    #print(elemento_tabla)
    #print()

    # Extraemos los elementos del body de la tabla correspondiente a cada uno de los registros de la misma. (con el punto le indicamos que tiene que buscar dentro del tbody y no en todo el html)
    Filas = elemento_tabla.find_elements(By.XPATH, './/tr')
    #print(Filas)
    #print()

    print(f'Se encontraron {len(Filas)} registros en la tabla de cedears')
    print()

    # Creamos una lista para guardar la informacion extraida de la web
    cotizacion_cedear_diaria = []

    # Para cada elemento o fila le pedimos qe nos extraiga el texto con .text y separamos el string por espacios para que nos deje dos elementos correspondientes a fecha y referencia (precio del dolar). Luego lo agregamos a la lista "dolar_historico_base" que creamos anteriormente.
    for fila in Filas:
        texto = fila.text
        lista_separada = texto.split()
        cotizacion_cedear_diaria.append(lista_separada)

    # Eliminamos la primer fila de nombres
    cotizacion_cedear_diaria.pop(0)
    #print(cotizacion_cedear_diaria)

    # Creamos una copia para manipularla con libertad y no generar errores a futuro
    cotizacion_cedear_diaria2 = cotizacion_cedear_diaria.copy()

    # Del campo referencia cambiamos la coma ',' por punto '.' para luego modificar el tipo de dato de str a float y guardamos la info en la variable dolar_historico. En este punto ya tenemos lista la informacion para pasarla a la base de datos
    for i in range(len(cotizacion_cedear_diaria)):

        if cotizacion_cedear_diaria2[i][1] == '-':
            cotizacion_cedear_diaria2[i][1] = 0
        else:
            cotizacion_cedear_diaria2[i][1] = float(cotizacion_cedear_diaria[i][1].replace(".",'').replace(",","."))
        
        if cotizacion_cedear_diaria2[i][2] == '-':
            cotizacion_cedear_diaria2[i][2] = 0
        else:
            cotizacion_cedear_diaria2[i][2] = float(cotizacion_cedear_diaria[i][2].replace(".",'').replace(",","."))

        if cotizacion_cedear_diaria2[i][3] == '-':
            cotizacion_cedear_diaria2[i][3] = 0
        else:
            cotizacion_cedear_diaria2[i][3] = float(cotizacion_cedear_diaria[i][3].replace(".",'').replace(",","."))

        if cotizacion_cedear_diaria2[i][4] == '-':
            cotizacion_cedear_diaria2[i][4] = 0
        else:
            cotizacion_cedear_diaria2[i][4] = float(cotizacion_cedear_diaria[i][4].replace(".",'').replace(",","."))

        if cotizacion_cedear_diaria2[i][5] == '-':
            cotizacion_cedear_diaria2[i][5] = 0
        else:
            cotizacion_cedear_diaria2[i][5] = float(cotizacion_cedear_diaria[i][5].replace(".",'').replace(",","."))
        
        if cotizacion_cedear_diaria2[i][6] == '-':
            cotizacion_cedear_diaria2[i][6] = 0
        else:
            cotizacion_cedear_diaria2[i][6] = float(cotizacion_cedear_diaria[i][6].replace(".",'').replace(",","."))
        
        if cotizacion_cedear_diaria2[i][7] == '-':
            cotizacion_cedear_diaria2[i][7] = 0
        else:
            cotizacion_cedear_diaria2[i][7] = float(cotizacion_cedear_diaria[i][7].replace(".",'').replace(",","."))
        
        if cotizacion_cedear_diaria2[i][8] == '-':
            cotizacion_cedear_diaria2[i][8] = 0
        else:
            cotizacion_cedear_diaria2[i][8] = float(cotizacion_cedear_diaria[i][8].replace(".",'').replace(",","."))

        if cotizacion_cedear_diaria2[i][10] == '-':
            cotizacion_cedear_diaria2[i][10] = 0
        else:
            cotizacion_cedear_diaria2[i][10] = float(cotizacion_cedear_diaria[i][10].replace(".",'').replace(",","."))

        if cotizacion_cedear_diaria2[i][11] == '-':
            cotizacion_cedear_diaria2[i][11] = 0
        else:
            cotizacion_cedear_diaria2[i][11] = float(cotizacion_cedear_diaria[i][11].replace(".",'').replace(",","."))
        
        if cotizacion_cedear_diaria2[i][12] == '-':
            cotizacion_cedear_diaria2[i][12] = 0
        else:
            cotizacion_cedear_diaria2[i][12] = cotizacion_cedear_diaria[i][12].replace(".",'').replace(",",".")

        if cotizacion_cedear_diaria2[i][13] == '-':
            cotizacion_cedear_diaria2[i][13] = 0
        else:
            cotizacion_cedear_diaria2[i][13] = float(cotizacion_cedear_diaria[i][13].replace(".",'').replace(",","."))

        #print(cotizacion_cedear_diaria2[i])    

    return cotizacion_cedear_diaria2


def obtener_acciones_lider(driver,url='https://www.rava.com/cotizaciones/acciones-argentinas'):
    
    # Con el driver previamente cargado, se abre chrome, y le pasamos la url que queremos que entre 
    driver.get(url)

    time.sleep(15)

    # Clickeamos el boton "GENERAL" para cambiar al grafico de cotizacion del cuadro general
    driver.find_element(By.XPATH,'/html/body/div[1]/main/div/div/div[2]/div/div/ul/li[1]/a').click()

    time.sleep(5)

    # Ubicamos el elemento tbody de la tabla de dolar historico con XPATH (camino html para ubicar el elemento). Busca solo el contenido, no las etiquetas
    elemento_tabla_lider = driver.find_element(By.XPATH, '//table[@id="table"]')
    #print(elemento_tabla)
    #print()

    # Extraemos los elementos del body de la tabla correspondiente a cada uno de los registros de la misma. (con el punto le indicamos que tiene que buscar dentro del tbody y no en todo el html)
    Filas_lider = elemento_tabla_lider.find_elements(By.XPATH, './/tr')
    #print(Filas)
    #print()

    print(f'Se encontraron {len(Filas_lider)} registros en la tabla acciones argentinas lideres')
    print()

    # Creamos una lista para guardar la informacion extraida de la web
    cotizacion_acciones_arg_diaria_lider = []
    #cotizacion_acciones_arg_diaria_general = []

    # Para cada elemento o fila le pedimos qe nos extraiga el texto con .text y separamos el string por espacios para que nos deje dos elementos correspondientes a fecha y referencia (precio del dolar). Luego lo agregamos a la lista "dolar_historico_base" que creamos anteriormente.
    for fila_lider in Filas_lider[1:]:
        texto_lider = fila_lider.text
        lista_separada_lider = texto_lider.split()
        cotizacion_acciones_arg_diaria_lider.append(lista_separada_lider)

    
    # Creamos una copia para manipularla con libertad y no generar errores a futuro
    cotizacion_acciones_arg_diaria2_lider = cotizacion_acciones_arg_diaria_lider.copy()

    # Del campo referencia cambiamos la coma ',' por punto '.' para luego modificar el tipo de dato de str a float y guardamos la info en la variable dolar_historico. En este punto ya tenemos lista la informacion para pasarla a la base de datos
    for i in range(len(cotizacion_acciones_arg_diaria_lider)):

        if cotizacion_acciones_arg_diaria2_lider[i][1] == '-':
            cotizacion_acciones_arg_diaria2_lider[i][1] = 0
        else:
            cotizacion_acciones_arg_diaria2_lider[i][1] = float(cotizacion_acciones_arg_diaria_lider[i][1].replace(".",'').replace(",","."))
        
        if cotizacion_acciones_arg_diaria2_lider[i][2] == '-':
            cotizacion_acciones_arg_diaria2_lider[i][2] = 0
        else:
            cotizacion_acciones_arg_diaria2_lider[i][2] = float(cotizacion_acciones_arg_diaria_lider[i][2].replace(".",'').replace(",","."))

        if cotizacion_acciones_arg_diaria2_lider[i][3] == '-':
            cotizacion_acciones_arg_diaria2_lider[i][3] = 0
        else:
            cotizacion_acciones_arg_diaria2_lider[i][3] = float(cotizacion_acciones_arg_diaria_lider[i][3].replace(".",'').replace(",","."))

        if cotizacion_acciones_arg_diaria2_lider[i][4] == '-':
            cotizacion_acciones_arg_diaria2_lider[i][4] = 0
        else:
            cotizacion_acciones_arg_diaria2_lider[i][4] = float(cotizacion_acciones_arg_diaria_lider[i][4].replace(".",'').replace(",","."))

        if cotizacion_acciones_arg_diaria2_lider[i][5] == '-':
            cotizacion_acciones_arg_diaria2_lider[i][5] = 0
        else:
            cotizacion_acciones_arg_diaria2_lider[i][5] = float(cotizacion_acciones_arg_diaria_lider[i][5].replace(".",'').replace(",","."))
        
        if cotizacion_acciones_arg_diaria2_lider[i][6] == '-':
            cotizacion_acciones_arg_diaria2_lider[i][6] = 0
        else:
            cotizacion_acciones_arg_diaria2_lider[i][6] = float(cotizacion_acciones_arg_diaria_lider[i][6].replace(".",'').replace(",","."))
        
        if cotizacion_acciones_arg_diaria2_lider[i][7] == '-':
            cotizacion_acciones_arg_diaria2_lider[i][7] = 0
        else:
            cotizacion_acciones_arg_diaria2_lider[i][7] = float(cotizacion_acciones_arg_diaria_lider[i][7].replace(".",'').replace(",","."))
        
        if cotizacion_acciones_arg_diaria2_lider[i][8] == '-':
            cotizacion_acciones_arg_diaria2_lider[i][8] = 0
        else:
            cotizacion_acciones_arg_diaria2_lider[i][8] = float(cotizacion_acciones_arg_diaria_lider[i][8].replace(".",'').replace(",","."))

        if cotizacion_acciones_arg_diaria2_lider[i][10] == '-':
            cotizacion_acciones_arg_diaria2_lider[i][10] = 0
        else:
            cotizacion_acciones_arg_diaria2_lider[i][10] = float(cotizacion_acciones_arg_diaria_lider[i][10].replace(".",'').replace(",","."))

        if cotizacion_acciones_arg_diaria2_lider[i][11] == '-':
            cotizacion_acciones_arg_diaria2_lider[i][11] = 0
        else:
            cotizacion_acciones_arg_diaria2_lider[i][11] = float(cotizacion_acciones_arg_diaria_lider[i][11].replace(".",'').replace(",","."))

        cotizacion_acciones_arg_diaria2_lider[i].append('Lider')

        #print(cotizacion_acciones_arg_diaria2_lider[i])    
            
    #print(cotizacion_acciones_arg_diaria2_lider)

    return cotizacion_acciones_arg_diaria2_lider


def obtener_acciones_general(driver,url='https://www.rava.com/cotizaciones/acciones-argentinas'):
    
    # Con el driver previamente cargado, se abre chrome, y le pasamos la url que queremos que entre 
    driver.get(url)

    time.sleep(15)
    
    # Clickeamos el boton "GENERAL" para cambiar al grafico de cotizacion del cuadro general
    driver.find_element(By.XPATH,'/html/body/div[1]/main/div/div/div[2]/div/div/ul/li[2]/a').click()
    
    time.sleep(5)

    # Ubicamos el elemento tbody de la tabla de dolar historico con XPATH (camino html para ubicar el elemento). Busca solo el contenido, no las etiquetas
    elemento_tabla_general = driver.find_element(By.XPATH, '//table[@id="table"]')
    #print(elemento_tabla)
    #print()

    # Extraemos los elementos del body de la tabla correspondiente a cada uno de los registros de la misma. (con el punto le indicamos que tiene que buscar dentro del tbody y no en todo el html)
    Filas_general = elemento_tabla_general.find_elements(By.XPATH, './/tr')
    #print(Filas)
    #print()

    print(f'Se encontraron {len(Filas_general)} registros en la tabla acciones argentinas general')
    print()

    cotizacion_acciones_arg_diaria_general = []

    # Para cada elemento o fila le pedimos qe nos extraiga el texto con .text y separamos el string por espacios para que nos deje dos elementos correspondientes a fecha y referencia (precio del dolar). Luego lo agregamos a la lista "dolar_historico_base" que creamos anteriormente.
    for fila_general in Filas_general[1:]:
        texto_general = fila_general.text
        lista_separada_general = texto_general.split()
        cotizacion_acciones_arg_diaria_general.append(lista_separada_general)
            
    
    # Creamos una copia para manipularla con libertad y no generar errores a futuro
    cotizacion_acciones_arg_diaria2_general = cotizacion_acciones_arg_diaria_general.copy()

    # Del campo referencia cambiamos la coma ',' por punto '.' para luego modificar el tipo de dato de str a float y guardamos la info en la variable dolar_historico. En este punto ya tenemos lista la informacion para pasarla a la base de datos
    for i in range(len(cotizacion_acciones_arg_diaria_general)):

        if cotizacion_acciones_arg_diaria2_general[i][1] == '-':
            cotizacion_acciones_arg_diaria2_general[i][1] = 0
        else:
            cotizacion_acciones_arg_diaria2_general[i][1] = float(cotizacion_acciones_arg_diaria_general[i][1].replace(".",'').replace(",","."))
        
        if cotizacion_acciones_arg_diaria2_general[i][2] == '-':
            cotizacion_acciones_arg_diaria2_general[i][2] = 0
        else:
            cotizacion_acciones_arg_diaria2_general[i][2] = float(cotizacion_acciones_arg_diaria_general[i][2].replace(".",'').replace(",","."))

        if cotizacion_acciones_arg_diaria2_general[i][3] == '-':
            cotizacion_acciones_arg_diaria2_general[i][3] = 0
        else:
            cotizacion_acciones_arg_diaria2_general[i][3] = float(cotizacion_acciones_arg_diaria_general[i][3].replace(".",'').replace(",","."))

        if cotizacion_acciones_arg_diaria2_general[i][4] == '-':
            cotizacion_acciones_arg_diaria2_general[i][4] = 0
        else:
            cotizacion_acciones_arg_diaria2_general[i][4] = float(cotizacion_acciones_arg_diaria_general[i][4].replace(".",'').replace(",","."))

        if cotizacion_acciones_arg_diaria2_general[i][5] == '-':
            cotizacion_acciones_arg_diaria2_general[i][5] = 0
        else:
            cotizacion_acciones_arg_diaria2_general[i][5] = float(cotizacion_acciones_arg_diaria_general[i][5].replace(".",'').replace(",","."))
        
        if cotizacion_acciones_arg_diaria2_general[i][6] == '-':
            cotizacion_acciones_arg_diaria2_general[i][6] = 0
        else:
            cotizacion_acciones_arg_diaria2_general[i][6] = float(cotizacion_acciones_arg_diaria_general[i][6].replace(".",'').replace(",","."))
        
        if cotizacion_acciones_arg_diaria2_general[i][7] == '-':
            cotizacion_acciones_arg_diaria2_general[i][7] = 0
        else:
            cotizacion_acciones_arg_diaria2_general[i][7] = float(cotizacion_acciones_arg_diaria_general[i][7].replace(".",'').replace(",","."))
        
        if cotizacion_acciones_arg_diaria2_general[i][8] == '-':
            cotizacion_acciones_arg_diaria2_general[i][8] = 0
        else:
            cotizacion_acciones_arg_diaria2_general[i][8] = float(cotizacion_acciones_arg_diaria_general[i][8].replace(".",'').replace(",","."))

        if cotizacion_acciones_arg_diaria2_general[i][10] == '-':
            cotizacion_acciones_arg_diaria2_general[i][10] = 0
        else:
            cotizacion_acciones_arg_diaria2_general[i][10] = float(cotizacion_acciones_arg_diaria_general[i][10].replace(".",'').replace(",","."))

        if cotizacion_acciones_arg_diaria2_general[i][11] == '-':
            cotizacion_acciones_arg_diaria2_general[i][11] = 0
        else:
            cotizacion_acciones_arg_diaria2_general[i][11] = float(cotizacion_acciones_arg_diaria_general[i][11].replace(".",'').replace(",","."))

        cotizacion_acciones_arg_diaria2_general[i].append('General')
        #print(cotizacion_acciones_arg_diaria2_general[i])            

    #print(cotizacion_acciones_arg_diaria2_general)

    return  cotizacion_acciones_arg_diaria2_general


#-----------------------------------------------------------------------------------------------------------------------------------------------#

def Carga_cedears_bd(cotizacion_cedear):
    # Conectamos la base de datos
    conn = bd.conexion_base_de_datos()

    # Creamos el cursos para poder impactar la base de datos
    cur = conn.cursor()
    #print('Conectado el cursor!')
    #print()

    # Creamos la sentencia SQL
    upsert_query = """
        INSERT INTO public.rava_cotizacion_cedear_diaria
            (ticker,
            ultima_cotizacion,
            porcentaje_gan_dia,
            porcentaje_gan_mes,
            porcentaje_gan_año,
            cotizacion_anterior,
            cotizacion_apertura,
            cotizacion_minimo,
            cotizacion_maximo,
            hora,
            vol_nominal,
            vol_efectivo,
            ratio,
            ccl)
        VALUES(%s, %s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker)
        DO update set 
            ticker = EXCLUDED.ticker
            , ultima_cotizacion = EXCLUDED.ultima_cotizacion
            , porcentaje_gan_dia = EXCLUDED.porcentaje_gan_dia
            , porcentaje_gan_mes = EXCLUDED.porcentaje_gan_mes
            , porcentaje_gan_año = EXCLUDED.porcentaje_gan_año
            , cotizacion_anterior = EXCLUDED.cotizacion_anterior
            , cotizacion_apertura = EXCLUDED.cotizacion_apertura
            , cotizacion_minimo = EXCLUDED.cotizacion_minimo
            , cotizacion_maximo = EXCLUDED.cotizacion_maximo
            , hora = EXCLUDED.hora
            , vol_nominal = EXCLUDED.vol_nominal
            , vol_efectivo = EXCLUDED.vol_efectivo
            , ratio = EXCLUDED.ratio
            , ccl = EXCLUDED.ccl
        ;
    """

    # Hacemos un bucle for para asociar cada fila con la sentencia sql
    conteo = 0

    try:
        for data in cotizacion_cedear:
            cur.execute(upsert_query, data)
            conteo += 1
            #print(data)
            #print(conteo)
            #print('Insert realizado con exito!')  
        
        # Commit de los cambios
        conn.commit()
        #print('Listo el commit!')
        #print()

        # Cerramos el cursor
        cur.close()
        #print('Cerramos el cursor')
        #print()

    except Exception as e:
        print(f'Error al insertar los registros: {e}')  
        
        # Commit de los cambios
        conn.rollback()
        #print('Listo el rollback!')
        #print()

        # Cerramos el cursor
        cur.close()
        #print('Cerramos el cursor')
        #print()      


    print(f'Se revisaron, insertaron o updetearon {conteo} registros con exito')
    print()


#-----------------------------------------------------------------------------------------------------------------------------------------------#

def Carga_acciones_lider_bd(cotizacion_acciones_lider):
    # Conectamos la base de datos
    conn = bd.conexion_base_de_datos()

    # Creamos el cursos para poder impactar la base de datos
    cur = conn.cursor()
    #print('Conectado el cursor!')
    #print()

    # Creamos la sentencia SQL
    upsert_query = """
        INSERT INTO public.rava_cotizacion_acciones_arg_lider
            (ticker,
            ultima_cotizacion,
            porcentaje_gan_dia,
            porcentaje_gan_mes,
            porcentaje_gan_año,
            cotizacion_anterior,
            cotizacion_apertura,
            cotizacion_minimo,
            cotizacion_maximo,
            hora,
            vol_nominal,
            vol_efectivo,
            tipo_accion)
        VALUES(%s, %s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker)
        DO update set 
            ticker = EXCLUDED.ticker
            , ultima_cotizacion = EXCLUDED.ultima_cotizacion
            , porcentaje_gan_dia = EXCLUDED.porcentaje_gan_dia
            , porcentaje_gan_mes = EXCLUDED.porcentaje_gan_mes
            , porcentaje_gan_año = EXCLUDED.porcentaje_gan_año
            , cotizacion_anterior = EXCLUDED.cotizacion_anterior
            , cotizacion_apertura = EXCLUDED.cotizacion_apertura
            , cotizacion_minimo = EXCLUDED.cotizacion_minimo
            , cotizacion_maximo = EXCLUDED.cotizacion_maximo
            , hora = EXCLUDED.hora
            , vol_nominal = EXCLUDED.vol_nominal
            , vol_efectivo = EXCLUDED.vol_efectivo
            ,tipo_accion = EXCLUDED.tipo_accion
        ;
    """

    # Hacemos un bucle for para asociar cada fila con la sentencia sql
    conteo = 0

    try:
        for data in cotizacion_acciones_lider:
            cur.execute(upsert_query, data)
            conteo += 1
            #print(data)
            #print(conteo)
            #print('Insert realizado con exito!')  
        
        # Commit de los cambios
        conn.commit()
        #print('Listo el commit!')
        #print()

        # Cerramos el cursor
        cur.close()
        #print('Cerramos el cursor')
        #print()

    except Exception as e:
        print(f'Error al insertar los registros: {e}')  
        
        # Commit de los cambios
        conn.rollback()
        #print('Listo el rollback!')
        #print()

        # Cerramos el cursor
        cur.close()
        #print('Cerramos el cursor')
        #print()      

    print(f'Se revisaron, insertaron o updetearon {conteo} registros con exito')
    print()



#-----------------------------------------------------------------------------------------------------------------------------------------------#

def Carga_acciones_gral_bd(cotizacion_acciones_gral):
    # Conectamos la base de datos
    conn = bd.conexion_base_de_datos()

    # Creamos el cursos para poder impactar la base de datos
    cur = conn.cursor()
    #print('Conectado el cursor!')
    #print()

    # Creamos la sentencia SQL
    upsert_query = """
        INSERT INTO public.rava_cotizacion_acciones_arg_gral
            (ticker,
            ultima_cotizacion,
            porcentaje_gan_dia,
            porcentaje_gan_mes,
            porcentaje_gan_año,
            cotizacion_anterior,
            cotizacion_apertura,
            cotizacion_minimo,
            cotizacion_maximo,
            hora,
            vol_nominal,
            vol_efectivo,
            tipo_accion)
        VALUES(%s, %s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
        ON CONFLICT (ticker)
        DO update set 
            ticker = EXCLUDED.ticker
            , ultima_cotizacion = EXCLUDED.ultima_cotizacion
            , porcentaje_gan_dia = EXCLUDED.porcentaje_gan_dia
            , porcentaje_gan_mes = EXCLUDED.porcentaje_gan_mes
            , porcentaje_gan_año = EXCLUDED.porcentaje_gan_año
            , cotizacion_anterior = EXCLUDED.cotizacion_anterior
            , cotizacion_apertura = EXCLUDED.cotizacion_apertura
            , cotizacion_minimo = EXCLUDED.cotizacion_minimo
            , cotizacion_maximo = EXCLUDED.cotizacion_maximo
            , hora = EXCLUDED.hora
            , vol_nominal = EXCLUDED.vol_nominal
            , vol_efectivo = EXCLUDED.vol_efectivo
            ,tipo_accion = EXCLUDED.tipo_accion
        ;
    """

    # Hacemos un bucle for para asociar cada fila con la sentencia sql
    conteo = 0

    try:
        for data in cotizacion_acciones_gral:
            cur.execute(upsert_query, data)
            conteo += 1
            #print(data)
            #print(conteo)
            #print('Insert realizado con exito!')  
        
        # Commit de los cambios
        conn.commit()
        #print('Listo el commit!')
        #print()

        # Cerramos el cursor
        cur.close()
        #print('Cerramos el cursor')
        #print()

    except Exception as e:
        print(f'Error al insertar los registros: {e}')  
        
        # Commit de los cambios
        conn.rollback()
        #print('Listo el rollback!')
        #print()

        # Cerramos el cursor
        cur.close()
        #print('Cerramos el cursor')
        #print()      

    print(f'Se revisaron, insertaron o updetearon {conteo} registros con exito')
    print()    

#-----------------------------------------------------------------------------------------------------------------------------------------------#
   
def proceso_general_rava():
    
    '''# Seteamos las opciones de chrome para la navegacion (instanciamos la clase)
    options = webdriver.ChromeOptions()

    # Le agregamos la opcion para que abra el chrome en modo incongnito
    options.add_argument('--incognito')

    # Maximizar el navegador
    options.add_argument("--start-maximized")

    # Instalamos el driver de chrome atraves del manager y abrimos el chrome
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
'''

    driver = iniciar_scraping()
    #-----------------------------------------------------------------------------------------------------------------------------------------------#
    # FUNCIONES

    cotizacion_cedear = obtener_cedears(driver=driver,url='https://www.rava.com/cotizaciones/cedears')

    cotizacion_acciones_lider = obtener_acciones_lider(driver=driver, url='https://www.rava.com/cotizaciones/acciones-argentinas')

    cotizacion_acciones_gral = obtener_acciones_general(driver=driver, url='https://www.rava.com/cotizaciones/acciones-argentinas')

    #-----------------------------------------------------------------------------------------------------------------------------------------------#
    # CERRAMOS EL PROCESO
    # Cerramos el driver de chrome
    driver.close()

    print('Cerramos el driver!')
    print()

    Carga_cedears_bd(cotizacion_cedear)
    Carga_acciones_lider_bd(cotizacion_acciones_lider)
    Carga_acciones_gral_bd(cotizacion_acciones_gral)
    

#-----------------------------------------------------------------------------------------------------------------------------------------------#
# INICIAMOS EL PROCESO


if __name__ == '__main__':
    proceso_general_rava() 