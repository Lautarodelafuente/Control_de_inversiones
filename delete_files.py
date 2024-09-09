import shutil
import os

def delite_file_and_folders(folder_path):
    # Borra todo el contenido de la carpeta
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  # Elimina archivos o enlaces
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # Elimina carpetas y todo su contenido
            print(f'archivo {file_path} eliminado.')
        except Exception as e:
            print(f"No se pudo eliminar {file_path}. Error: {e}")


if __name__ == "__main__":
    folder_path = "C:/Users/USER/.wdm/drivers/chromedriver/win64"
    delite_file_and_folders(folder_path)