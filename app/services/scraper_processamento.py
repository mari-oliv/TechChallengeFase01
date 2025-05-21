import requests
from bs4 import BeautifulSoup
from app.core import logging_config
import logging
import pandas as pd
import sqlite3
from datetime import datetime

def get_processamento(year: int, option: int) -> pd.DataFrame:
    """
    Coleta dados da página de processamento do Vitibrasil usando scraping.

    Parâmetros:
        year (int): Ano do filtro da tabela.
        option (int): Opção do produto no site:
            01 - Viníferas
            02 - Americanas e híbridas
            03 - uvas de mesa
            04 - sem classificação
            
    Retorna:
        pd.DataFrame: Dados coletados do site para o ano e opção informados.
    """
    logging.info("Iniciando scraping de processamento.")
    URL = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={year}&opcao=opt_03&subopcao=subopt_0{option}"
    try:
        response = requests.get(URL,timeout=15)
        response.raise_for_status()
        response.encoding ='utf-8'
        logging.info("Acesso ao site bem-sucedido.")
    except Exception as e:
        logging.error(f"Erro ao acessar {URL}: {e}")
        return pd.DataFrame()
    
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="tb_base tb_dados")
    
    if not table: 
        product_tags = soup.find_all("button", class_="btn_sopt")
        logging.warning(f"Table not found for year {year}, option {option} (produto: {product_tags[option-1].text.strip() if len(product_tags) >= option else 'desconhecido'})")
        return pd.DataFrame()
    
    product_tags = soup.find_all("button", class_="btn_sopt")
    if len(product_tags) >= option:
        product = product_tags[option-1].text.strip()
    else:
        product = None
        
    rows = table.find_all("tr")
    data = []

    group = None
    col_sem_definicao = table.find_all("th",class_="tb_base tb_dados", string="Sem definição ")

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
        
        #logging.info(col_sem_definicao)
        if col_sem_definicao != []:
            data.append({
                "Year": year, 
                "GroupName": group
            })
        else:
                data.append({
                "Year": year, 
                "GroupName": group, 
                "Cultive": cultive,
                "Quantity_Kg": quantity,
                "Product": product
            })

    return pd.DataFrame(data) #salva dados

def save_data_db(df: pd.DataFrame) -> None:
    """
    Salva os dados coletados do site Vitibrasil no banco de dados SQLite, tabela 'processamento'.

    Parâmetros:
        df (pd.DataFrame): Dados coletados do site.

    Retorna:
        None
    """
    conn = sqlite3.connect("vitibrasil.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processamento (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Year INTEGER,
            GroupName TEXT, 
            Cultive TEXT,
            Quantity_Kg TEXT,
            Product TEXT
        )
    ''')#cria a table

    df.to_sql("processamento", conn, if_exists="append", index=False)#dataframe para sql
    conn.commit()
    conn.close()


def scrap_processamento() -> None:
    """
    Executa o scraping para todos os anos e opções de produto da página processamento, salvando os dados no banco de dados.

    Parâmetros:
        None

    Retorna:
        None
    """
    now = datetime.now().year
    for year in range(1970, now):
        logging.info(f"Extracting data year: {year}")
        for option in range(1,5):  
            df = get_processamento(year,option)
            if not df.empty:
                save_data_db(df)
                product = df["Product"].iloc[0]
                logging.info(f"{len(df)} dados de {product} salvos em 'processamento'.")
    
if __name__ == "__main__":
    """
        Para extrair os dados do site, execute no terminal:
        python -m app.services.scraper_processamento
    """
    logging_config()
    scrap_processamento()
