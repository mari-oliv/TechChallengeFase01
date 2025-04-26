from fastapi import APIRouter, Query
from typing import Optional
import sqlite3


router = APIRouter()

#rota da aba produtos
@router.get("/producao") #?year=70-2023
async def get_prod_data(year: int = Query(None, ge=1970, le=2023)): #filtragem de ano
    try:
        conn = sqlite3.connect("vitibrasil.db") #pega o banco
        cursor = conn.cursor()

        if year:
            query = "SELECT Year, Product, Quantity_L FROM prod WHERE Year = ?"
            cursor.execute(query, (year,)) #monta a query e executa
        
        else:
            query = "SELECT Year, Product, Quantity_L FROM prod"
            cursor.execute(query)
        
        rows = cursor.fetchall() #pega todos os dados da query

        data = [{"Year": row[0], "Product": row[1], "Quantity_L": row[2]} for row in rows]
        conn.close() #fecha conexao

        return {"success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"success": False, "error": str(e)}
    

def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value.replace(",", ".")) #substitui virgula por ponto
    except ValueError:
        return None

#rota da aba processamento, subselção viniferas
@router.get("/processamento/viniferas")
async def get_viniferas(
    year: int = Query(None, ge=1970, le=2023),
    group: str = Query(None),
    cultive: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None)
):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)

        conn = sqlite3.connect("vitibrasil_proc.db")
        cursor = conn.cursor()

        query = "SELECT Year, GroupName, Cultive, Quantity_Kg FROM proc_viniferas WHERE 1=1" #query inicial
        params = [] #armazena parametros da query
        if year is not None:
            query += " AND Year = ?"
            params.append(year) #adiciona ano a lista dos parametros criada

        if group:
            query += " AND GroupName LIKE ?"
            params.append(f"%{group}%") #adiciona grupo a lista dos parametros criada, utilizando o LIKE para nao precisar ser identico

        if cultive:
            query += " AND Cultive LIKE ?"
            params.append(f"%{cultive}%")

        if quant_min_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) >= ?"
            params.append(quant_min)

        if quant_max_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) <= ?"
            params.append(quant_max)
            
        
        cursor.execute(query, params)#monta a query e executa os filtros caso sejam passados
        rows = cursor.fetchall() #pega todos os dados da query

        data = [{"Year": row[0], "GroupName": row[1], "Cultive": row[2], "Quantity_Kg": row[3]} for row in rows]
        conn.close() #fecha conexao
        
        return {"success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"success": False, "error": str(e)}
