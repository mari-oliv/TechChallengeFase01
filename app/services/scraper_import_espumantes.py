import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from app.core import save_data
import logging

def get_import_espumantes(year: int) -> pd.DataFrame:
    import_data = []
    for option in range(5):
        URL = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={year}&opcao=opt_{option}&subopcao=subopt_05"
        response = requests.get(URL)
        if response.status_code != 202 or response.status_code != 200:
            response.encoding ='utf-8'
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            with open("meuarquivo.txt", "w", encoding="utf-8") as f:
                f.write(soup.prettify())

            table = soup.find("table", class_="tb_base tb_dados")
            
            product_tag = soup.find("button", class_='btn_sopt')
            
            product = product_tag.text.strip() if product_tag else None

            if not table:
                continue

            rows = table.find_all("tr")
            for row in rows:
                cols = row.find_all("td")
                if len(cols) != 3:
                    continue

                country = cols[0].text.strip()
                quantity = cols[1].text.strip()
                value = cols[2].text.strip()
                
                import_data.append({
                    "Year": year,
                    "Country": country, 
                    "Quantity_Kg": quantity, 
                    "Value_USD": value,
                    "Product": product,
                    "Page": "importacao"
                })
            else:
                logging.error('failed to connect: Vitibrasil website')
    return pd.DataFrame(import_data)

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
    all_data = []
    for year in range(1970, 2025):
        print(f"Extracting data year: {year}")
        df = get_import_espumantes(year)
        logging.info('checking df: %s',df)
        if not df.empty:
            all_data.append(df)
            logging.info('Data saved on import table')
        else:
            logging.error('Error: data not saved on import table')
            
    if all_data:
        logging.info('Data: %s', all_data)
        final_df = pd.concat(all_data, ignore_index=True)
        save_data(final_df, table="importacao")
    
    
if __name__ == "__main__":
    export_all_years_importacao()
    # python -m app.services.scraper_import_espumantes
