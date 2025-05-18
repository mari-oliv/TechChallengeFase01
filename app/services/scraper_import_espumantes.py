import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from app.core import init_db
import logging
import os

def get_import_espumantes(year: int, option: int) -> pd.DataFrame:
    data = []   
    URL = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={year}&opcao=opt_05&subopcao=subopt_0{option}"
    #logging.info('url %s', URL)
    response = requests.get(URL)
    response.encoding ='utf-8'
     
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="tb_base tb_dados")
    
    if not table: #se nao encontrar nao teve prod esse ano 'vazio'
        return pd.DataFrame()

    product_tag = soup.find("button", class_="btn_sopt")
    product = product_tag.text.strip() if product_tag else None
    
    rows = table.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        if len(cols) != 3:
            continue
        country = cols[0].text.strip()
        quantity = cols[1].text.strip()
        value = cols[2].text.strip()
        
        data.append({
            "Year": year,
            "Country": country, 
            "Quantity_Kg": quantity, 
            "Value_USD": value,
            "Product": product,
            "Page": "importacao"
        })
    return pd.DataFrame(data)

def save_at_db_importacao(df: pd.DataFrame) -> None:
    conn = sqlite3.connect("vitibrasil.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS importacao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Year INTEGER,
            Country TEXT,
            Quantity_Kg TEXT, 
            Value_USD TEXT,
            Product TEXT,
            Page TXT
        )
    ''')
    df.to_sql("importacao", conn, if_exists="append", index=False)

    conn.commit()
    conn.close()
    print("All data saved")

def export_all_years_importacao():
    all_data = []
    for year in range(1970, 2025):
        print(f"Extracting data year: {year}")
        for option in range(1,5):
            df = get_import_espumantes(year,option)
            all_data.append(df)
    if not df.empty:
        save_at_db_importacao(df)
        logging.info('Data saved on import table')
    else:
        logging.error('data not saved on import table because is empty')

if __name__ == "__main__":
    export_all_years_importacao()
    # python -m app.services.scraper_import_espumantes
