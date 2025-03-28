from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import logging
import os

logger = logging.getLogger(__name__)

def dir_descargas(driver, ruta):
    driver.command_executor_commands["send_chrome_command"] = ("post", "/session/$sessionId/chromium/send_command")
    params = {
        "cmd":"page.setDownloadBehavior",
        "params": {
            "behavior": "allow",
            "downloadPath": os.path.abspath(ruta)
        }
    }
    driver.execute("send_chrome_command", params)

def iniciar_scraping():
    # Seteamos las opciones de chrome para la navegacion (instanciamos la clase)
    options = webdriver.ChromeOptions()

    # Le agregamos la opcion para que abra el chrome en modo incongnito
    options.add_argument('--incognito')

    # Maximizar el navegador
    options.add_argument("--start-maximized")

    options.add_extension('F:\Lautaro\Programacion\Proyectos\Control_de_inversiones\Extentions\Adblock-Plus-bloqueador-de-anuncios-gratis.crx')

    # Instalamos el driver de chrome atraves del manager y abrimos el chrome
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    return driver


def iniciar_scraping_with_download():
    # Seteamos las opciones de chrome para la navegacion (instanciamos la clase)
    options = webdriver.ChromeOptions()

    # Le agregamos la opcion para que abra el chrome en modo incongnito
    #options.add_argument('--incognito')

    # Maximizar el navegador
    options.add_argument("--start-maximized")

    #options.add_extension('F:\Lautaro\Programacion\Proyectos\Control_de_inversiones\Extentions\Adblock-Plus-bloqueador-de-anuncios-gratis.crx')

    # Configure download preferences
    prefs = {
        "download.default_directory": f"{os.getcwd()}\Download",  # Change this to your desired download path
       }
    options.add_experimental_option("prefs", prefs)

    # Instalamos el driver de chrome atraves del manager y abrimos el chrome
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    return driver