from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import Base_de_datos_postgresql as bd
import psycopg2 as pg
import time

#-----------------------------------------------------------------------------------------------------------------------------------------------#

def obtener_dolar(driver):

    # Ubicamos el elemento tbody de la tabla de dolar historico con XPATH (camino html para ubicar el elemento). Busca solo el contenido, no las etiquetas
    elemento_tabla = driver.find_element(By.XPATH, '//tbody[@class="general-historical__tbody tbody"]')
    #print(elemento_tabla)
    #print()

    # Extraemos los elementos del body de la tabla correspondiente a cada uno de los registros de la misma. (con el punto le indicamos que tiene que buscar dentro del tbody y no en todo el html)
    elemento_tabla_dolar = elemento_tabla.find_elements(By.XPATH, './/tr')
    #print(elemento_tabla_dolar)
    #print()

    print(f'Se encontraron {len(elemento_tabla_dolar)} registros en la tabla')
    print()

    # Creamos una lista para guardar la informacion extraida de la web
    dolar_historico_base = []

    # Para cada elemento o fila le pedimos qe nos extraiga el texto con .text y separamos el string por espacios para que nos deje dos elementos correspondientes a fecha y referencia (precio del dolar). Luego lo agregamos a la lista "dolar_historico_base" que creamos anteriormente.
    for fila in elemento_tabla_dolar:
        texto = fila.text
        lista_separada = texto.split()
        dolar_historico_base.append(lista_separada)

    #print(dolar_historico_base)
    #print()

    # Creamos una copia para manipularla con libertad y no generar errores a futuro
    dolar_historico = dolar_historico_base.copy()

    # Del campo referencia cambiamos la coma ',' por punto '.' para luego modificar el tipo de dato de str a float y guardamos la info en la variable dolar_historico. En este punto ya tenemos lista la informacion para pasarla a la base de datos
    for i in range(len(dolar_historico_base)):
        dolar_historico[i][1] = float(dolar_historico_base[i][1].replace(",","."))
        #print(dolar_historico[i][1])

    #print(dolar_historico)
    print('Lista la correccion del tipo de dato de la referencia')
    print()

    # Cerramos el driver de chrome
    driver.close()

    print('Cerramos el driver!')
    print()

    return dolar_historico

#-----------------------------------------------------------------------------------------------------------------------------------------------#

def ultima_fecha(conn = bd.conexion_base_de_datos()):
    
    # Creamos el cursos para poder impactar la base de datos
    cur = conn.cursor()
    print('Conectado el cursor!')
    print()

    select_max_fecha = '''select max(fecha)::date from dolar_ccl_historico dch ;'''

    try:
        cur.execute(select_max_fecha)

        ultima_fecha_bd_dolar_ccl_historico = cur.fetchone()

        for fecha in ultima_fecha_bd_dolar_ccl_historico:
            ultima_fecha = fecha.strftime('%d/%m/%Y')

        # Cerramos el cursor
        cur.close()
        print('Cerramos el cursor')
        print()

    except Exception as e:
        print(f'Error al buscar la ultima fecha de la tabla dolar_ccl_historico: {e}') 

        # Cerramos el cursor
        cur.close()
        print('Cerramos el cursor')
        print()

    return ultima_fecha

#-----------------------------------------------------------------------------------------------------------------------------------------------#

def info_dolar_to_database(info_dolar,conn):

    # Creamos el cursos para poder impactar la base de datos
    cur = conn.cursor()
    print('Conectado el cursor!')
    print()

    # Creamos la sentencia SQL
    upsert_query = """
        INSERT INTO public.dolar_ccl_historico
        (fecha, referencia,fecha_actualizacion)
        values (%s,%s,now())
        ON CONFLICT (fecha)
        DO NOTHING;
    """

    # Hacemos un bucle for para asociar cada fila con la sentencia sql
    conteo = 0

    try:
        for data in info_dolar:
            cur.execute(upsert_query, data)
            conteo += 1
            #print(data)
            #print(conteo)
            #print('Insert realizado con exito!')  
        
        # Commit de los cambios
        conn.commit()
        print('Listo el commit!')
        print()

        # Cerramos el cursor
        cur.close()
        print('Cerramos el cursor')
        print()

    except Exception as e:
        print(f'Error al insertar los registros: {e}')  
        
        # Commit de los cambios
        conn.rollback()
        print('Listo el rollback!')
        print()

        # Cerramos el cursor
        cur.close()
        print('Cerramos el cursor')
        print()      

    print(f'Se revisaron, insertaron o updetearon {conteo} registros con exito')
    print()
    
#-----------------------------------------------------------------------------------------------------------------------------------------------#

def proceso_gral(url):

    # Seteamos las opciones de chrome para la navegacion (instanciamos la clase)
    options = webdriver.ChromeOptions()

    # Le agregamos la opcion para que abra el chrome en modo incongnito
    options.add_argument('--incognito')

    # Maximizar el navegador
    options.add_argument("--start-maximized")

    # Instalamos el driver de chrome atraves del manager y abrimos el chrome
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    # Con el driver previamente cargado, se abre chrome, y le pasamos la url que queremos que entre 
    driver.get(url)

    time.sleep(10)

    # Creamos la variable demora
    delay = 10

    # Corremos el proceso
    try:
        
        # Buscamos la fecha mas reciente de nuestra base de datos para usarla como fecha de inicio en la pagina
        ultima_fecha_registrada = ultima_fecha()

        #print(ultima_fecha_registrada)
        
        # Configuramos la fecha desde
        WebDriverWait(driver, delay).until(EC.element_to_be_clickable((By.XPATH, "//div[@class='general-historical__config config']/input[@class='general-historical__datepicker datepicker desde form-control']"))).send_keys(ultima_fecha_registrada)
        print('Se modifico la "FECHA DESDE" con exito!')
        print()

        time.sleep(10)

        # Clickeamos el boton "VER DATOS" para hacer efectiva nuestra seleccion
        driver.find_element(By.XPATH,'//button[@class="general-historical__button boton"]').click()
        print('Click en "VER DATOS"')
        print()

        time.sleep(10)

        # Introducimos una demora en el proceso hasta que ubique el elemento deseado para luego continuar
        tabla = WebDriverWait(driver,delay).until(EC.presence_of_element_located((By.XPATH,'//tbody[@class="general-historical__tbody tbody"]')))
        print('La pagina cargo con exito!')
        print()

        time.sleep(5)

        # Esperamos tener respuesta del elemento "tabla" o que el programa lo encuentre
        if tabla:

            # Obtenemos la informacion de la funcion que creamos anteriormente
            info_dolar = obtener_dolar(driver)

            print('Informacion extraida de la web y lista para subir a la base de datos:')
            print()
            print(info_dolar)
            print()
            print('scraping exitoso!')
            print()

            # Si info_dolar no esta vacio corremos el resto del proceso
            if info_dolar != []:
                info_dolar_to_database(info_dolar,bd.conexion_base_de_datos())
            else:
                print('No se encontraron resultados!')
                print()

    except TimeoutException:
        print('La pagina tardo demasiado en cargar')
        print()
        info_dolar = []

    except Exception as e:
        print(f'Ocurrio un error en el proceso principal: {e}')  
        print()
    
#-----------------------------------------------------------------------------------------------------------------------------------------------#

if __name__ == "__main__":
    
    print('iniciando proceso...')

    # Iniciamos el proceso pasandole el link de la url que queremos scrapear
    proceso_gral('https://www.ambito.com/contenidos/dolar-cl-historico.html')

    print('Fin del Proceso')