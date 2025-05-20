import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from app.core import logging_config, logging
from datetime import datetime

def get_comercializacao(year: int) -> pd.DataFrame:
    """
    Coleta dados da página de comercialização do Vitibrasil usando scraping.

    Parâmetros:
        year (int): Ano do filtro da tabela.

    Retorna:
        pd.DataFrame: Dados coletados do site para os anos informados.
    """
    URL = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={year}&opcao=opt_04" 
    
    try:
        response = requests.get(URL)
        response.raise_for_status()
        response.encoding ='utf-8'
    except Exception as e:
        logging.error(f"Erro ao acessar {URL}: {e}")
        return pd.DataFrame()
    
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="tb_base tb_dados")
    
    if not table:
        return pd.DataFrame()
    
    rows = table.find_all("tr")
    data = []
    group = None

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
            "Quantity_L": quantity
        })

    return pd.DataFrame(data) 

def save_data_db(df: pd.DataFrame) -> None:
    """
    Salva os dados coletados do site Vitibrasil no banco de dados SQLite, tabela 'comercializacao'.

    Parâmetros:
        df (pd.DataFrame): Dados coletados do site.

    Retorna:
        None
    """
    conn = sqlite3.connect("vitibrasil.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comercializacao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Year INTEGER,
            GroupName TEXT, 
            Cultive TEXT,
            Quantity_L TEXT
        )
    ''')

    df.to_sql("comercializacao", conn, if_exists="append", index=False)

    conn.commit()
    conn.close()


def scrap_comercializacao() -> None:
    """
    Executa o scraping na aba comercialização, para todos os anos e salva no banco de dados.

    Parâmetros:
        None

    Retorna:
        None
    """
    now = datetime.now().year
    for year in range(1970, now):
        logging.info(f"Extracting data year: {year}")
        df = get_comercializacao(year) #chama a function para cada ano 70's - 2024
        if not df.empty:
            save_data_db(df)
            logging.info(f"{len(df)} dados de salvos em 'comercializacao'.")
        else:
            logging.warning(f"Data not saved - empty DataFrame")
    
if __name__ == "__main__":
    """
        Para extrair os dados do site, execute no terminal:
        python -m app.services.scraper_comercializacao
    """
    logging_config()
    scrap_comercializacao()
