import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

def get_proc_viniferas_all_year(year: int) -> pd.DataFrame:
    URL = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={year}&opcao=opt_03&subopcao=subopt_01" #monta url dinamica
    response = requests.get(URL)
    response.encoding ='utf-8'
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", class_="tb_base tb_dados")
    if not table: #se nao encontrar nao teve prod esse ano 'vazio'
        return pd.DataFrame()
    
    rows = table.find_all("tr")
    data = [] #armazena data

    group = None #insere grupo

    for row in rows:
        cols = row.find_all("td")
        if len(cols) != 2:
            continue

        col1_class = cols[0].get("class", []) #css class de cada td

        if "tb_item" in col1_class: #identifica o grupo
            group = cols[0].text.strip()
            cultive = group
            quantity = cols[1].text.strip()
        elif "tb_subitem" in col1_class: #identifica subitem do grupo
            cultive = cols[0].text.strip()
            quantity = cols[1].text.strip()
        else:
            continue

        data.append({
            "Year": year, 
            "GroupName": group, 
            "Cultive": cultive,
            "Quantity_Kg": quantity
        })

    return pd.DataFrame(data) #salva dados

def save_at_db_viniferas(df: pd.DataFrame) -> None:
    conn = sqlite3.connect("vitibrasil_proc.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS proc_viniferas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Year INTEGER,
            GroupName TEXT, 
            Cultive TEXT,
            Quantity_Kg TEXT
        )
    ''')#cria a table

    df.to_sql("proc_viniferas", conn, if_exists="append", index=False)#dataframe para sql

    conn.commit()
    conn.close()
    print("All data saved")


def export_all_years_viniferas():
    
    for year in range(1970, 2024):
        print(f"Extracting data year: {year}")
        df = get_proc_viniferas_all_year(year) #chama a function para cada ano 70's - 2024
        if not df.empty:
            save_at_db_viniferas(df)
    
    
if __name__ == "__main__":
    export_all_years_viniferas()
