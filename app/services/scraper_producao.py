import logging
import pandas as pd
import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime
from app.core import logging_config
from fastapi.responses import JSONResponse

def get_producao(year: int) -> pd.DataFrame:
    """
    Coleta dados da página de producao do Vitibrasil usando scraping.

    Parâmetros:
        year (int): Ano do filtro da tabela.
    
    Retorna:
        pd.DataFrame: Dados coletados do site para o ano informado.
    """
    URL = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={year}&opcao=opt_02"
    
    try:
        logging.info("Acessando o site Vitibrasil")
        response = requests.get(URL)
        response.raise_for_status()
        response.encoding = 'utf-8' 
    except Exception as e:
        logging.error(f"Erro ao acessas {URL}: {e}")
        return pd.DataFrame()
    
    soup = BeautifulSoup(response.text, "html.parser") 
    table = soup.find("table", class_="tb_base tb_dados")
    
    if not table:
        return pd.DataFrame()

    rows = table.find_all("tr")
    data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) == 2:
            if "tb_item" in cols[0].get("class", []):
                current_product = cols[0].text.strip()
                total_quantity = cols[1].text.strip()
                data.append({
                    "Year": year,
                    "Category": current_product,
                    "Product": "Todos da categoria",
                    "Quantity_L": total_quantity
                })
                continue
            
            quantity = cols[1].text.strip()
            subProduct = cols[0].text.strip()
            data.append({
                "Year": year,
                "Category": current_product if 'current_product' in locals() else None,
                "Product": subProduct,
                "Quantity_L": quantity
            })

    return pd.DataFrame(data)

def save_at_db(df: pd.DataFrame) -> None:
    """
    Salva os dados coletados do site Vitibrasil no banco de dados SQLite, tabela 'producao'.

    Parâmetros:
        df (pd.DataFrame): Dados coletados do site.

    Retorna:
        None
    """
    conn = sqlite3.connect("vitibrasil.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS producao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Year INTEGER,
            Category TEXT, 
            Product TEXT,
            Quantity_L TEXT
        )
        ''')
    
    df.to_sql("producao", conn, if_exists="append", index=False)
    conn.commit()
    conn.close()

def scrap_producao() -> None:
    """
    Executa o scraping para todos os anos da produto da página producao, salvando os dados no banco de dados.

    Parâmetros:
        None

    Retorna:
        None
    """
    now = datetime.now().year
    for year in range(1970, now):
        print(f"Extracting data from year {year}")
        df = get_producao(year) 
        if not df.empty:
            save_at_db(df)


if __name__ == "__main__":
    """
        Para extrair os dados do site, execute no terminal:
        python -m app.services.scraper_producao
    """
    logging_config()
    scrap_producao()


