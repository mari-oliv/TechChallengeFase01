import requests
from bs4 import BeautifulSoup
from typing import List, Dict
import pandas as pd

def get_prod_data_year(year: int) -> pd.DataFrame:
    URL = f"http://vitibrasil.cnpuv.embrapa.br/index.php??ano={year}&opcao=opt_02"
    response = requests.get(URL)
    response.encoding = 'utf-8' 
    soup = BeautifulSoup(response.text, "html.parser")

    tables = soup.find_all("table")
    if not  tables:
        return pd.DataFrame()

    dfs = pd.read_html(str(tables[0])) #get the first table
    df = dfs[0]
    df["Ano"] = year
    return df

def export_all_years():
    all_dfs = []
    for year in range(1970, 2024):
        print(f"Extracting data from year {year}")
        df = get_prod_data_year(year)
        if not df.empty:
            all_dfs.append(df)
    
    df_final = pd.concat(all_dfs, ignore_index=True)
    df_final.to_excel("data_prod.xlsx", index=False)
    print("All data exported")


if __name__ == "__main__":
    export_all_years()