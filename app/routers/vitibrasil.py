from fastapi import APIRouter, Query, Depends, HTTPException, Form
from typing import Optional
import sqlite3
from app.util.auth import verifica_token
from app.util.auth import cria_token

router = APIRouter()


USER_TEST = {
    "username": "admin",
    "password": "admin"
}

@router.post("/token")
async def login(username: str = Form(...), password: str = Form(...)):
    if username != USER_TEST["username"] or password != USER_TEST["password"]:
        raise HTTPException(status_code=401, detail="As credenciais sao invalidas")
    
    access_token = cria_token(data={"sub": username})
    return {"access_token": access_token, "token_type": "bearer"}


#rota da aba produtos
@router.get("/producao") #?year=70-2023
async def get_prod_data(year: int = Query(None, ge=1970, le=2023), #filtragem de ano
    token_user: str = Depends(verifica_token)):
    try:
        conn = sqlite3.connect("vitibrasil_prod.db") #pega o banco
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
    

#funtcion for parsing kg values
def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value.replace(",", ".")) #substitui virgula por ponto
    except ValueError:
        return None
    
#rota da aba processamento, subseleção viniferas
@router.get("/processamento/viniferas")
async def get_proc_viniferas(
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


#rota da aba processamento, subseleção americanas e hibridas
@router.get("/processamento/viniferas/americanas&hibridas")
async def get_proc_ame_hib(
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

        query = "SELECT Year, GroupName, Cultive, Quantity_Kg FROM proc_viniferas_ame_hib WHERE 1=1" #query inicial
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
    

#rota da aba processamento, subseleção uvas de mesa
@router.get("/processamento/viniferas/uvas_de_mesa")
async def get_proc_uvas_mesa(
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

        query = "SELECT Year, GroupName, Cultive, Quantity_Kg FROM proc_viniferas_uvas_mesa WHERE 1=1" #query inicial
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
    

#rota da aba processamento, subselecao sem classificacao
@router.get("/processamento/viniferas/sem_classificacao")
async def get_proc_sem_class(
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

        query = "SELECT Year, SemClass, Cultive, Quantity_Kg FROM proc_viniferas_sem_class WHERE 1=1" #query inicial
        params = [] #lista dos filtros

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if group:
            query += " AND SemClass LIKE ?"
            params.append(group)
        
        if cultive:
            query += " AND Cultive LIKE ?"
            params.append(group)
        
        if quant_min_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) >= ?"
            params.append(quant_min)
        
        if quant_max_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) <= ?"
            params.append(quant_max)
        

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "SemClass": row[1], "Cultive": row[2], "Quantity": row[3]} for row in rows]
        conn.close()

        return {"success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"success": False, "error": str(e)}
    

#rota da aba comercializacao
@router.get("/comercializacao")
async def get_comercializacao(
    year: int = Query(None, ge=1970, le=2023),
    group: str = Query(None),
    cultive: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None)
):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)

        conn = sqlite3.connect("vitibrasil_comer.db")
        cursor = conn.cursor()

        query = "SELECT Year, GroupName, Cultive, Quantity_L FROM comercializacao WHERE 1=1" #query inicial
        params = [] #lista dos filtros

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if group:
            query += " AND GroupName LIKE ?"
            params.append(group)
        
        if cultive:
            query += " AND Cultive LIKE ?"
            params.append(group)
        
        if quant_min_value is not None:
            query += " AND CAST(REPLACE(Quantity_L, '.', '') AS REAL) >= ?"
            params.append(quant_min)
        
        if quant_max_value is not None:
            query += " AND CAST(REPLACE(Quantity_L, '.', '') AS REAL) <= ?"
            params.append(quant_max)
        

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "GroupName": row[1], "Cultive": row[2], "Quantity": row[3]} for row in rows]
        conn.close()

        return {"success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"success": False, "error": str(e)}
    

#rota para aba de importacao de vinhos de mesa
@router.get("/importacao/vinhos_mesa")
async def get_import_vinhos_mesa(
    year: int = Query(None, ge= 1970, le= 2024),
    country: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None),
    value_min: Optional[str] = Query(None),
    value_max: Optional[str] = Query(None)
):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil_import.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM importacao_vinhos WHERE 1=1" #query inicial
        params = [] #lista dos filtros

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if country:
            query += " AND Country LIKE ?"
            params.append(country)
        
        if quant_min_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) >= ?"
            params.append(quant_min)

        if quant_max_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) <= ?"
            params.append(quant_max)
        
        if value_min_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) >= ?"
            params.append(value_min)
        
        if value_max_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) <= ?"
            params.append(value_max)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "Country": row[1], "Quantity_Kg": row[2], "Value_USD": row[3]} for row in rows]
        conn.close()

        return {"Success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"Success": False, "error": str(e)}


#rota para aba de importacao de espumantes
@router.get("/importacao/espumantes")
async def get_import_espumantes(
    year: int = Query(None, ge= 1970, le= 2024),
    country: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None),
    value_min: Optional[str] = Query(None),
    value_max: Optional[str] = Query(None)
):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil_import.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM importacao_espumantes WHERE 1=1" #query inicial
        params = [] #lista dos filtros

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if country:
            query += " AND Country LIKE ?"
            params.append(country)
        
        if quant_min_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) >= ?"
            params.append(quant_min)

        if quant_max_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) <= ?"
            params.append(quant_max)
        
        if value_min_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) >= ?"
            params.append(value_min)
        
        if value_max_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) <= ?"
            params.append(value_max)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "Country": row[1], "Quantity_Kg": row[2], "Value_USD": row[3]} for row in rows]
        conn.close()

        return {"Success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"Success": False, "error": str(e)}
    

#rota para aba de importacao de uvas frescas
@router.get("/importacao/uvas_frescas")
async def get_import_uvas_frescas(
    year: int = Query(None, ge= 1970, le= 2024),
    country: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None),
    value_min: Optional[str] = Query(None),
    value_max: Optional[str] = Query(None)
):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil_import.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM importacao_uvas_frescas WHERE 1=1" #query inicial
        params = [] #lista dos filtros

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if country:
            query += " AND Country LIKE ?"
            params.append(country)
        
        if quant_min_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) >= ?"
            params.append(quant_min)

        if quant_max_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) <= ?"
            params.append(quant_max)
        
        if value_min_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) >= ?"
            params.append(value_min)
        
        if value_max_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) <= ?"
            params.append(value_max)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "Country": row[1], "Quantity_Kg": row[2], "Value_USD": row[3]} for row in rows]
        conn.close()

        return {"Success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"Success": False, "error": str(e)}
    

#rota para aba de importacao de uvas passas
@router.get("/importacao/uvas_passas")
async def get_import_uvas_passas(
    year: int = Query(None, ge= 1970, le= 2024),
    country: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None),
    value_min: Optional[str] = Query(None),
    value_max: Optional[str] = Query(None)
):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil_import.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM importacao_uvas_passas WHERE 1=1" #query inicial
        params = [] #lista dos filtros

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if country:
            query += " AND Country LIKE ?"
            params.append(country)
        
        if quant_min_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) >= ?"
            params.append(quant_min)

        if quant_max_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) <= ?"
            params.append(quant_max)
        
        if value_min_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) >= ?"
            params.append(value_min)
        
        if value_max_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) <= ?"
            params.append(value_max)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "Country": row[1], "Quantity_Kg": row[2], "Value_USD": row[3]} for row in rows]
        conn.close()

        return {"Success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"Success": False, "error": str(e)}

        
 #rota para aba de importacao de suco de uva
@router.get("/importacao/suco_de_uva")
async def get_import_suco_de_uva(
    year: int = Query(None, ge= 1970, le= 2024),
    country: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None),
    value_min: Optional[str] = Query(None),
    value_max: Optional[str] = Query(None)
):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil_import.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM importacao_suco_de_uva WHERE 1=1" #query inicial
        params = [] #lista dos filtros

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if country:
            query += " AND Country LIKE ?"
            params.append(country)
        
        if quant_min_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) >= ?"
            params.append(quant_min)

        if quant_max_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) <= ?"
            params.append(quant_max)
        
        if value_min_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) >= ?"
            params.append(value_min)
        
        if value_max_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) <= ?"
            params.append(value_max)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "Country": row[1], "Quantity_Kg": row[2], "Value_USD": row[3]} for row in rows]
        conn.close()

        return {"Success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"Success": False, "error": str(e)}   
    

 #rota para aba de exportacao vinhos de mesa
@router.get("/exportacao/vinho_mesa")
async def get_export_vinho_mesa(
    year: int = Query(None, ge= 1970, le= 2024),
    country: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None),
    value_min: Optional[str] = Query(None),
    value_max: Optional[str] = Query(None)
):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil_export.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM exportacao_vinhos WHERE 1=1" #query inicial
        params = [] #lista dos filtros

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if country:
            query += " AND Country LIKE ?"
            params.append(country)
        
        if quant_min_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) >= ?"
            params.append(quant_min)

        if quant_max_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) <= ?"
            params.append(quant_max)
        
        if value_min_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) >= ?"
            params.append(value_min)
        
        if value_max_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) <= ?"
            params.append(value_max)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "Country": row[1], "Quantity_Kg": row[2], "Value_USD": row[3]} for row in rows]
        conn.close()

        return {"Success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"Success": False, "error": str(e)}  
    

 #rota para aba de exportacao espumantes
@router.get("/exportacao/espumantes")
async def get_export_espumantes(
    year: int = Query(None, ge= 1970, le= 2024),
    country: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None),
    value_min: Optional[str] = Query(None),
    value_max: Optional[str] = Query(None)
):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil_export.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM exportacao_espumantes WHERE 1=1" #query inicial
        params = [] #lista dos filtros

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if country:
            query += " AND Country LIKE ?"
            params.append(country)
        
        if quant_min_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) >= ?"
            params.append(quant_min)

        if quant_max_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) <= ?"
            params.append(quant_max)
        
        if value_min_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) >= ?"
            params.append(value_min)
        
        if value_max_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) <= ?"
            params.append(value_max)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "Country": row[1], "Quantity_Kg": row[2], "Value_USD": row[3]} for row in rows]
        conn.close()

        return {"Success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"Success": False, "error": str(e)} 
    

 #rota para aba de exportacao uvas frescas
@router.get("/exportacao/uvas_frescas")
async def get_export_uvas_frescas(
    year: int = Query(None, ge= 1970, le= 2024),
    country: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None),
    value_min: Optional[str] = Query(None),
    value_max: Optional[str] = Query(None)
):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil_export.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM exportacao_uvas_frescas WHERE 1=1" #query inicial
        params = [] #lista dos filtros

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if country:
            query += " AND Country LIKE ?"
            params.append(country)
        
        if quant_min_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) >= ?"
            params.append(quant_min)

        if quant_max_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) <= ?"
            params.append(quant_max)
        
        if value_min_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) >= ?"
            params.append(value_min)
        
        if value_max_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) <= ?"
            params.append(value_max)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "Country": row[1], "Quantity_Kg": row[2], "Value_USD": row[3]} for row in rows]
        conn.close()

        return {"Success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"Success": False, "error": str(e)} 
    

 #rota para aba de exportacao suco de uva
@router.get("/exportacao/suco_de_uva")
async def get_export_uvas_frescas(
    year: int = Query(None, ge= 1970, le= 2024),
    country: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None),
    value_min: Optional[str] = Query(None),
    value_max: Optional[str] = Query(None)
):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil_export.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM exportacao_suco_de_uva WHERE 1=1" #query inicial
        params = [] #lista dos filtros

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if country:
            query += " AND Country LIKE ?"
            params.append(country)
        
        if quant_min_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) >= ?"
            params.append(quant_min)

        if quant_max_value is not None:
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) <= ?"
            params.append(quant_max)
        
        if value_min_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) >= ?"
            params.append(value_min)
        
        if value_max_quant is not None:
            query += " AND CAST(REPLACE(Value_USD, '.', '') AS REAL) <= ?"
            params.append(value_max)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "Country": row[1], "Quantity_Kg": row[2], "Value_USD": row[3]} for row in rows]
        conn.close()

        return {"Success": True, "total": len(data), "data": data}
    
    except Exception as e:
        return {"Success": False, "error": str(e)} 
    


    



    

