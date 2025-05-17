import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from app.core import save_at_db_importacao
import logging

def get_import_espumantes(year: int) -> pd.DataFrame:
    all_data = []
    for idx in range(5):
        logging.info(idx)
        URL = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={year}&opcao=opt_{idx}&subopcao=subopt_05"
        response = requests.get(URL)
        response.encoding ='utf-8'
        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table", class_="tb_base tb_dados")
        product_tag = soup.find("button", class_='btn_sopt')
        product = product_tag.text.strip() if product_tag else None

        if not table:
            continue  # pula para o próximo idx

        rows = table.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            if len(cols) != 3:
                continue

            country = cols[0].text.strip()
            quantity = cols[1].text.strip()
            value = cols[2].text.strip()

            all_data.append({
                "Year": year,
                "Country": country, 
                "Quantity_Kg": quantity, 
                "Value_USD": value,
                "Product": product
            })

    return pd.DataFrame(all_data) #salva dados

"""def save_at_db_importacao(df: pd.DataFrame) -> None:
    conn = sqlite3.connect("vitibrasil_import.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS importacao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Year INTEGER,
            Country TEXT,
            Quantity_Kg TEXT, 
            Value_USD TEXT,
            Product TEXT
        )
    ''')#cria a table

    df.to_sql("importacao", conn, if_exists="append", index=False)

    conn.commit()
    conn.close()
    print("All data saved")
"""

def export_all_years_importacao():
    
    for year in range(1970, 2025):
        print(f"Extracting data year: {year}")
        all_data = get_import_espumantes(year) #chama a function para cada ano 70's - 2024
        if not all_data.empty:
            logging.info('salvando no banco')
            save_at_db_importacao(all_data)
    
    
if __name__ == "__main__":
    export_all_years_importacao()
    # python -m app.services.scraper_import_espumantes
