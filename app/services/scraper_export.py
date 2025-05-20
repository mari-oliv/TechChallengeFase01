
from bs4 import BeautifulSoup
from datetime import datetime
from app.core import logging_config
import logging
import pandas as pd
import requests
import sqlite3

def get_export(year: int, option: int) -> pd.DataFrame:
    """
    Coleta dados da página de importação do Vitibrasil usando scraping.

    Parâmetros:
        year (int): Ano do filtro da tabela.
        option (int): Opção do produto no site:
            01 - vinho de mesa
            02 - espumantes
            03 - uvas frescas
            04 - suco de uva

    Retorna:
        pd.DataFrame: Dados coletados do site para o ano e opção informados.
    """
    URL = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={year}&opcao=opt_06&subopcao=subopt_0{option}" 
    try:
        response = requests.get(URL)
        response.raise_for_status()
        response.encoding ='utf-8'
    except Exception as e:
        logging.error(f"Erro ao acessar {URL}: {e}")
        return pd.DataFrame()
        
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="tb_base tb_dados")
    product_tags = soup.find_all("button", class_="btn_sopt")
    if len(product_tags) >= option:
        product = product_tags[option-1].text.strip()
    else:
        product = None
        
    if not table:
        return pd.DataFrame()
    
    rows = table.find_all("tr")
    data = [] 

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
            "Page": "exportacao"
        })

    return pd.DataFrame(data) 

def save_data_db(df: pd.DataFrame) -> None:
    """
    Salva os dados coletados do site Vitibrasil no banco de dados SQLite, tabela 'exportacao'.

    Parâmetros:
        df (pd.DataFrame): Dados coletados do site.

    Retorna:
        None
    """
    conn = sqlite3.connect("vitibrasil.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exportacao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Year INTEGER,
            Country TEXT,
            Quantity_Kg TEXT, 
            Value_USD TEXT,
            Product TEXT,
            Page TXT
        )
    ''')#cria a table

    df.to_sql("exportacao", conn, if_exists="append", index=False)
    conn.commit()
    conn.close()
    
def scrap_exportacao() -> None:
    """
    Executa o scraping para todos os anos e opções de produto da página exportação, salvando os dados no banco de dados.

    Parâmetros:
        None

    Retorna:
        None
    """
    now = datetime.now().year
    for year in range(1970, now):
        logging.info(f"Extracting data year: {year}")
        for option in range(1, 5):
            df = get_export(year, option)
            page = df["Page"].iloc[0] if not df.empty else "desconhecido"
            if not df.empty:
                product = df["Product"].iloc[0]
                save_data_db(df)
                logging.info(f"{len(df)} dados de {product} salvos em 'exportacao'.")
            else:
                logging.warning(f"Data not saved - page: {page}) - empty DataFrame")
                
    
if __name__ == "__main__":
    """
        Para extrair os dados do site, execute no terminal:
        python -m app.services.scraper_export
    """
    logging_config()
    scrap_exportacao()
