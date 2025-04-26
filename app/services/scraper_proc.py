import requests
from bs4 import BeautifulSoup
import pandas as pd

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
            cultivar = group
            quantidade = cols[1].text.strip()
        elif "tb_subitem" in col1_class: #identifica subitem do grupo
            cultivar = cols[0].text.strip()
            quantidade = cols[1].text.strip()
        else:
            continue

        data.append({
            "Ano": year, 
            "Grupo": group, 
            "Cultivo": cultivar,
            "Quantidade(Kg)": quantidade
        })

    return pd.DataFrame(data) #salva dados

def export_all_years_viniferas():
    all = []
    for year in range(1970, 2024):
        print(f"Extracting data year: {year}")
        df = get_proc_viniferas_all_year(year) #chama a function para cada ano 70's - 2024
        if not df.empty:
            all.append(df)
    
    df_final = pd.concat(all, ignore_index=True)
    df_final.to_excel("proc_viniferas.xlsx", index=False)
    print("All data exported")

if __name__ == "__main__":
    export_all_years_viniferas()
