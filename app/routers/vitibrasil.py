from fastapi import APIRouter, Query
import pandas as pd


router = APIRouter()


@router.get("/producao") #?year=70-2023
async def get_prod_data(year: int = Query(None, ge=1970, le=2023)): #filtragem de ano
    try:
        df = pd.read_excel("data_prod.xlsx") #carregando modelo
        if year:
            df = df[df["Ano"] == year] #filtra df apenas se passar year

        data = df.to_dict(orient="records") #converte para Json
        return {"success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"success": False, "error": str(e)}
    

@router.get("/processamento/viniferas")
async def get_viniferas(
    year: int = Query(None, ge=1970, le=2023),
    group: str = Query(None),
    cultive: str = Query(None),
    quant_min: float = Query(None),
    quant_max: float = Query(None)
):
    try:
        df = pd.read_excel("proc_viniferas.xlsx")
    
        if year:
            df = df[df["Ano"] == year]
        if group:
            df = df[df["Grupo"] == group]
        if cultive:
            df = df[df["Cultivar"].str.contains(cultive, case=False, na=False)]
        if quant_min is not None:
            df["Quantidade(Kg)"] = df["Quantidade(Kg)"].str.strip().replace({r"[^\d.]": ""}, regex=True)
            df["Quantidade(Kg)"] = pd.to_numeric(df["Quantidade(Kg)"], errors='coerce')
            df = df[df["Quantidade(Kg)"] >= quant_min]
        if quant_max is not None:
            if not pd.api.types.is_numeric_dtype(df["Quantidade(Kg)"]):
                df["Quantidade(Kg)"] = df["Quantidade(Kg)"].str.strip().replace({r"[^\d.]": ""}, regex=True)
                df["Quantidade(Kg)"] = pd.to_numeric(df["Quantidade(Kg)"], errors='coerce')
            df = df[df["Quantidade(Kg)"] <= quant_max]

        data = df.to_dict(orient="records")
        return {"success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"success": False, "error": str(e)}
