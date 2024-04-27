from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager


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