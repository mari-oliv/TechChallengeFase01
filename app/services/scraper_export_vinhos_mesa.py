import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

def get_export_vinhos_de_mesa(year: int) -> pd.DataFrame:
    URL = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={year}&opcao=opt_06&subopcao=subopt_01" #monta url dinamica
    response = requests.get(URL)
    response.encoding ='utf-8'
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", class_="tb_base tb_dados")
    if not table: #se nao encontrar nao teve prod esse ano 'vazio'
        return pd.DataFrame()
    
    rows = table.find_all("tr")
    data = [] #armazena data

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
            "Value_USD": value
        })

    return pd.DataFrame(data) #salva dados

def save_at_db_exportacao(df: pd.DataFrame) -> None:
    conn = sqlite3.connect("vitibrasil_export.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exportacao_vinhos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Year INTEGER,
            Country TEXT,
            Quantity_Kg TEXT, 
            Value_USD TEXT
        )
    ''')#cria a table

    df.to_sql("exportacao_vinhos", conn, if_exists="append", index=False)#dataframe para sql

    conn.commit()
    conn.close()
    print("All data saved")


def export_all_years_exportacao():
    
    for year in range(1970, 2025):
        print(f"Extracting data year: {year}")
        df = get_export_vinhos_de_mesa(year) #chama a function para cada ano 70's - 2024
        if not df.empty:
            save_at_db_exportacao(df)
    
    
if __name__ == "__main__":
    export_all_years_exportacao()
