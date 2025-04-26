import requests
from bs4 import BeautifulSoup
from typing import List, Dict

URL = "http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_01"

def get_producao_data() -> List[Dict]:
    response = requests.get(URL)
    response.encoding = 'utf-8'  # força encoding correto
    soup = BeautifulSoup(response.text, "html.parser")

    tables = soup.find_all("table")
    
    data = []

    for table in tables:
        headers = [th.text.strip() for th in table.find_all("th")]
        for row in table.find_all("tr")[1:]:  # pula o header
            cols = [td.text.strip() for td in row.find_all("td")]
            if cols:
                entry = dict(zip(headers, cols))
                data.append(entry)

    return data

if __name__ == "__main__":
    dados = get_producao_data()
    for item in dados[:5]:
        print(item)

