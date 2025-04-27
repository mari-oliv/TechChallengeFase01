import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3


def get_prod_data_year(year: int) -> pd.DataFrame: #funcao recebe ano
    URL = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={year}&opcao=opt_02" #monta a url dinamica
    response = requests.get(URL)
    response.encoding = 'utf-8' 
    soup = BeautifulSoup(response.text, "html.parser") #cria objeto
    table = soup.find("table", class_="tb_base tb_dados") #encontra a table
    if not table:
        return pd.DataFrame()

    rows = table.find_all("tr")
    data = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) == 2:
            product = cols[0].text.strip() #verifica prod tira espacos
            quantity = cols[1].text.strip() ##verifica quant tira espacos
            data.append({
                "Year": year, 
                "Product": product,
                "Quantity_L": quantity
            })

    return pd.DataFrame(data) #adc um dict com os atributos da lista, salva no df

def save_at_db(df: pd.DataFrame) -> None:
    conn = sqlite3.connect("vitibrasil_prod.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS prod (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Year INTEGER,
            Product TEXT, 
            Quantity_L TEXT
        )
''')#cria a table
    
    df.to_sql("prod", conn, if_exists="append", index=False)#dataframe para sql

    conn.commit()
    conn.close()
    print("All data saved")

def export_all_years():
    for year in range(1970, 2024):
        print(f"Extracting data from year {year}")
        df = get_prod_data_year(year) #para cada ano chama a funcao
        if not df.empty:
            save_at_db(df)


if __name__ == "__main__":
    export_all_years()
    

