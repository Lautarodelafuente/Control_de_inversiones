
## COMO CREAR EJECUTABLES .EXE EN PYTHON:
Ver el siguiente video ==> https://www.youtube.com/watch?v=OTsBv9wMC6o

- Abrir el path donde estan los archivos ==> cd path
- activar el entorno virtual con ==> .\Scripts\activate
- Actualizar la libreria ==> pip install -u pyinstaller
- volver a path donde estan los archivos ==> cd path
- correr el siguiente codigo donde main.py es el archivo principal del script: ==> pyinstaller --onefile main.py


## COMO LEER Y ESCRIBIR EN GOOGLE SHEATS:
Ver el siguiente video ==> https://tesel.mx/como-usar-python-para-leer-y-escribir-datos-a-una-hoja-de-calculo-de-google-sheets-8879/ 

## CREAR EL ARCHIVO .ENV 
# Credenciales para la base de datos de postgresql
POSTGRES_HOST={Cargar IP o host}
POSTGRES_PORT={Cargar puerto}
POSTGRES_USER={Cargar Usuario}
POSTGRES_PASSWORD={Cargar Contraseña}
POSTGRES_DBNAME={Cargar nombre de la base de datos}

# Credenciales IOL
USER={Cargar usuario de iol}
PASS={Cargar contraseña de iol}

# Credenciales google
PATH_GOOGLE={Cargar el path donde guardamos las credenciales de google sheat. Ejemplo: path\google}

# Id de la hoja de calculos de google a trabajar
MIS_INVERSIONES={Id del archivo de google sheats}

# Carpeta donde se enviarán los logs del proceso main.py
Carpeta_Logs={path\logs}

## CREAR ARCHIVO .GITIGNORE Y GUARDE EL .ENV Y TODOS LOS ARCHIVOS QUE NO QUIERA QUE SE HAGAN PUBLICOS
