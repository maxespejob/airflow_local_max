from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import pandas as pd
import time
def web_scraping_bnr(url:str) -> pd.DataFrame:
    # Configurar Selenium (asegúrate de tener ChromeDriver instalado)
    service = Service("/usr/bin/chromedriver")  # Cambia esta ruta a la ubicación de tu chromedriver
    driver = webdriver.Chrome(service=service)

    # URL de la página
    url_bnr = url
    driver.get(url_bnr)
    time.sleep(15)
    # Esperar a que la página cargue (puedes ajustar según sea necesario)
    driver.implicitly_wait(10)

    # Obtener el HTML de la página después de cargar
    html = driver.page_source
    driver.quit()

    # Parsear el contenido HTML con Beautiful Soup
    soup = BeautifulSoup(html, 'html.parser')

    # Buscar la tabla específica usando su clase
    table = soup.find('table', class_="table-color max-h table")

    # Extraer los encabezados (que están todos en la fila <tr> principal con <th>)
    headers = [header.text.strip() for header in table.find('tr').find_all('th')]

    # Extraer las filas de datos
    rows = []
    for row in table.find_all('tr')[1:]:  # Excluir la primera fila que contiene los encabezados
        cells = row.find_all(['th', 'td'])  # Incluye el <th> del mes y los <td> de las celdas
        row_data = [cell.text.strip() for cell in cells]
        rows.append(row_data)

    # Crear un DataFrame con los encabezados y las filas extraídas
    df = pd.DataFrame(rows, columns=headers)

    return df[["Month","EUR","USD"]].head(1)
