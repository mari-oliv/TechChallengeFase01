import requests
from bs4 import BeautifulSoup
import pandas as pd


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
            produto = cols[0].text.strip() #verifica prod tira espacos
            quantidade = cols[1].text.strip() ##verifica quant tira espacos
            data.append({
                "Ano": year, 
                "Produto": produto,
                "Quantidade (L)": quantidade
            })

    return pd.DataFrame(data) #adc um dict com os atributos da lista, salva no df


def export_all_years():
    all_dfs = []
    for year in range(1970, 2024):
        print(f"Extracting data from year {year}")
        df = get_prod_data_year(year) #para cada ano chama a funcao
        if not df.empty:
            all_dfs.append(df)
    
    df_final = pd.concat(all_dfs, ignore_index=True)
    df_final.to_excel("data_prod.xlsx", index=False)
    print("All data exported")


if __name__ == "__main__":
    export_all_years()
    

