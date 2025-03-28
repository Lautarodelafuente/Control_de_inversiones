import shutil
import os
import logging

logger = logging.getLogger(__name__)

def delite_files_and_folders(folder_path):
    # Borra todo el contenido de la carpeta
    if not os.path.exists(folder_path):
        logger.warning(f"La carpeta '{folder_path}' no existe.")
        return
   
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  # Elimina archivos o enlaces
                logging.info(f"Archivo eliminado: {file_path}")
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)  # Elimina carpetas y todo su contenido
                logging.info(f"Carpeta eliminada: {file_path}")
        except Exception as e:
            logging.error(f"No se pudo eliminar {file_path}. Error: {e}")


if __name__ == "__main__":
    folder_path = "C:/Users/USER/.wdm/drivers/chromedriver/win64"
    delite_files_and_folders(folder_path)