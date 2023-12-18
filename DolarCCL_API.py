# Librerias
import requests
import pandas as pd
from datetime import datetime
import Base_de_datos_postgresql as bd

print()
print('#--------------------------------------------------------------------------------#')
print()

#--------------------------------------------------------------------------------#
#CONEXION DATABASE

conn = bd.conexion_base_de_datos()

# Creamos el cursos para poder impactar la base de datos
cur = conn.cursor()

#--------------------------------------------------------------------------------#
#API DOLAR CCL

def dolarCCL_api_datos():
    # Define the API URL
    api_url = "https://dolarapi.com/v1/dolares"

    # Send a GET request
    response = requests.get(api_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response (assuming the API returns JSON)
        data = response.json()

        # Creamos el dataframe con la informacion de la api
        df = pd.DataFrame(data)

        #print(df)

        # Iteramos para cambiar el valor del campo fechaActualizacion por la fecha y hora actual del proceso
        for i in range(len(df)):
            df.at[i,'fechaActualizacion'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        # Nos traemos solo la fila del dolarCCL y reseteamos el index (no es necesario lo del index)    
        cotizacion_diaria_dolar = df[df['casa']=='contadoconliqui'].reset_index(drop=True)

        #print(cotizacion_diaria_dolar)

    else:
        print("Request failed with status code:", response.status_code)

    # Retornamos los valores de la api
    return cotizacion_diaria_dolar

#--------------------------------------------------------------------------------#
# INSERTAR VALORES DE LA API EN LA BASE DE DATOS

def insert_dolarCCL_api():

    # Nos traemos los datos de la api
    cotizacion_diaria_dolar = dolarCCL_api_datos()

    # Pasamos los datos de la api a un tupla para poder trabajarlos con psycopg2. Nos traemos solo los datos que nos sirven para buscarlos en la bd y eliminarlos si los encontramos.
    cotizacion_delete = [tuple(x) for x in cotizacion_diaria_dolar[['casa','nombre','fechaActualizacion']].to_records(index=False)]

    # Creamos una tupla con los indices del dataframe.
    cotizacion_diaria_dolar_columns = [tuple(x) for x in cotizacion_diaria_dolar[['casa','nombre','compra','venta','fechaActualizacion']].to_records(index=False)]

    #print(cotizacion_diaria_dolar_columns)

    #print()

    # Creamos la sentencia SQL para insertar los valores
    insert_query = """
        INSERT INTO public.cotizacion_diaria_dolar(
        casa, nombre,compra, venta,  fechaactualizacion)
        VALUES (%s,%s,%s,%s,%s);
    """

    # Creamos la sentencia SQL para borrar los valores duplicados
    delete_query = """delete from public.cotizacion_diaria_dolar where casa = %s and nombre = %s and fechaactualizacion::date = %s::date;"""

    
    # Hacemos un bucle for para asociar cada fila con la sentencia sql que borra los datos
    try: 
        for data in cotizacion_delete:
            cur.execute(delete_query, data)
            print('Borramos los registros duplicados')

    except Exception as e:
        print(f'Error al borrar los registros: {e}')

    print()

   
    # Hacemos un bucle for para asociar cada fila con la sentencia sql que inserta los datos
    try: 
        for data in cotizacion_diaria_dolar_columns:
            cur.execute(insert_query, data)
            print('Insert realizado con exito!')
    
    except Exception as e:
        print(f'Error al borrar los registros: {e}')    

    print()

    
    print('Proceso finalizado!')

    print()

    # Commit de los cambios
    conn.commit()
    print('Listo el commit!')

    print()

    # Cerramos el cursor
    cur.close()

    # Cerramos la conexion a la base de datos
    conn.commit()

    print('Conexion cerrada con exito!')

    print()

    print('#--------------------------------------------------------------------------------#')

    print()

if __name__ == '__main__':
    insert_dolarCCL_api()    