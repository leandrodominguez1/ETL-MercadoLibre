import requests
import json
import csv
from datetime import date

DATE = str(date.today()).replace('-','')
# La URL de la API


def scrape_function():
    url = "https://api.mercadolibre.com/sites/MLA/search?category=MLA1071#json"
# Realizar la solicitud GET a la API    
    response = requests.get(url)
# Convertir la respuesta de la API en un objeto JSON
    data = json.loads(response.text.replace(u'\xa0', ' '))
# Hacer algo con los datos
    with open("plugins/operators/resultados.tsv", "w", newline='') as f:
        writer = csv.writer(f, delimiter='\t')    
        # Escribir la cabecera del archivo
        #writer.writerow(["id", "site_id", "title", "price", "sold_quantity", "thumbnail", "created_date"])   
        # Escribir cada resultado en el archivo
        for result in data["results"][:50]:
            writer.writerow([result["id"], result["site_id"], result["title"], result["price"], result["sold_quantity"], result["thumbnail"], DATE])


#scrape_function()