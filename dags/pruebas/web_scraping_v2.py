import requests
from bs4 import BeautifulSoup

def web_scraping_bnr() -> any:
    # URL del endpoint
    url = 'https://www.bnr.ro//blocks'

    # Payload encontrado en Network -> Payload
    payload = {
        'bid': '26412',
        'currentSlug': '23793-exchange-rates-monthly-quarterly-and-annual-averages',
        'cat_id': ''
    }

    # Cabeceras HTTP necesarias
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://www.bnr.ro/en/23793-exchange-rates-monthly-quarterly-and-annual-averages',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    }

    # Hacer la petición POST
    response = requests.post(url, data=payload, headers=headers)

    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')

        # Encontrar la tabla
        table = soup.find('table')
        if table:
            # Extraer encabezados
            headers = [h.get_text(strip=True) for h in table.find('thead').find_all('th')]

            # Extraer filas
            rows_data = []
            for row in table.find('tbody').find_all('tr'):
                # La primera celda es un <th> con la fecha
                month = row.find('th').get_text(strip=True)
                # El resto son <td>
                cells = row.find_all('td')
                data = [cell.get_text(strip=True) for cell in cells]
                # Combinar
                row_values = [month] + data
                rows_data.append(row_values)

            # Combinar cabeceras y filas en una lista
            full_table = [headers] + rows_data

            # Retornar la lista completa
            return full_table[:13]
        else:
            return "No se encontró la tabla en la respuesta."
    else:
        return f"La petición falló con el código de estado {response.status_code}."
