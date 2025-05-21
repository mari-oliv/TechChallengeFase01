from fastapi import APIRouter, Query, Depends, HTTPException, Form
from typing import Optional
import sqlite3
from app.util.auth import verifica_token
from app.util.auth import cria_token
from app.util.auth import hash_pass
from app.util.auth import verifica_pass
from app.core.database_config import init_db
from app.core.logging_config import logging_config
import logging
from pydantic import BaseModel
from app.services.scraper_producao import get_producao
from app.services.scraper_processamento import get_processamento

router = APIRouter()
logging_config()
class UserRequest(BaseModel):
    username: str
    password: str


@router.get("/")
async def root():
    return {"msg": "Vitibrasil API is aliiive"}


#cria usuario para poder capturar token
@router.post("/signup")
async def signup(user: UserRequest):
    await init_db()
    logging.info('Iniciando sign-up')
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()

    hashed_pw = hash_pass(user.password)

    try:
        cursor.execute('''
            INSERT INTO users (username, password) VALUES (?, ?)
        ''', (user.username, hashed_pw))
        conn.commit()
        logging.info(f"Usuário {user.username} cadastrado com sucesso.")
        return {"message": "Usuário cadastrado com sucesso!"}
    except sqlite3.IntegrityError:
        logging.error(f"Usuário {user.username} já existe.")
        raise HTTPException(status_code=202, detail="Usuário já existe.")
    finally:
        conn.close()

#rota para capturar token
@router.post("/token")
async def login(username: str = Form(...), password: str = Form(...)):
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()

    cursor.execute("SELECT password FROM users WHERE username = ?", (username,))
    result = cursor.fetchone()
    conn.close()

    if not result or not verifica_pass(password, result[0]):
        raise HTTPException(status_code=401, detail="As credenciais sao invalidas")
    
    access_token = cria_token(data={"sub": username})
    return {"access_token": access_token, "token_type": "bearer"}


#rota da aba produtos
@router.get("/producao/{year}") #?year=70-2023
async def get_prod_data(
    year: int,  # agora é path param
    product: Optional[str] = Query(None),  # pode ser query param
    token_user: str = Depends(verifica_token)
):
    try:
        #raise Exception("Simulação de site offline")
        df = get_producao(year)
        data = df.to_dict(orient="records")
        logging.info("Dados do site coletados com sucesso")
        if product:
            filtered_data = [row for row in data if row.get("Product") and product.lower() in row["Product"].lower()]
            return {"success": True, "total": len(filtered_data), "data": filtered_data}
        return {"success": True, "total": len(data), "data": data}
    except Exception as e:
        logging.error(f"Erro ao capturar dados do banco: {e}")
        return {"Success": False, "error": str(e)}
    except:
        logging.info("Erro ao capturar dados do site, tentando coletar do banco")
        conn = sqlite3.connect("vitibrasil.db") #pega o banco
        cursor = conn.cursor()

        if year and product:
            logging.info(f"Capturando dados do banco para o ano {year} e produto {product}")
            query = "SELECT Year, Product, Quantity_L FROM producao WHERE Year = ? AND Product LIKE ?"
            cursor.execute(query, (year,product)) #monta a query e executa
        
        else:
            query = "SELECT Year, Product, Quantity_L FROM producao WHERE Year = ?"
            cursor.execute(query,(year,))

        rows = cursor.fetchall() #pega todos os dados da query

        if not rows:
            logging.warning("Consulta ao banco realizada, mas nenhum dado encontrado.")
            conn.close()
            return {"success": True, "total": 0, "data": [], "message": "Nenhum dado encontrado no banco para os filtros informados."}

        data = [{"Year": row[0], "Product": row[1], "Quantity_L": row[2]} for row in rows]
        conn.close()
        return {"success": True, "total": len(data), "data": data}

#funtcion for parsing kg values
def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value.replace(",", ".")) #substitui virgula por ponto
    except ValueError:
        return None
    
#rota da aba processamento, subseleção viniferas
@router.get("/processamento/")
async def processamento(
    product: Optional[str] = Query(None),
    year: int = Query(None, ge=1970, le=2023),
    group: str = Query(None),
    cultive: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None)
,
token_user: str = Depends(verifica_token)):
    if product == 'Viníferas':
        option = 1
    elif product == 'Uvas de mesa':
        option = 2 
    elif product == 'Americanas e Híbridas':
        option = 3
    elif product == 'Sem Classificação':
        option = 4
    elif product == None:
        for i in range(1, 5):
            option = i
    else:
        pass
    
    try:
        df = get_processamento(year,option)
        data = df.to_dict(orient="records")
        logging.info("Dados do site coletados com sucesso")
        return {"success": True, "total": len(data), "data": data}
    except Exception as e:
        logging.error(f"Erro ao capturar dados do banco: {e}")
        return {"Success": False, "error": str(e)}
    except:
        logging.error("Erro ao capturar dados do site, tentando coletar do banco")
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)

        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, GroupName, Cultive, Quantity_Kg FROM processamento WHERE 1=1" #query inicial
        params = [] #armazena parametros da query

        if year is not None:
            query += " AND Year = ?"
            params.append(year) #adiciona ano a lista dos parametros criada

        if year and product:
            query += "AND Year = ? AND Product LIKE ?"
            params.append(year)
            params.append(f"%{product}%")

        if group is not None:
            query += " AND GroupName LIKE ?"
            params.append(f"%{group}%") #adiciona grupo a lista dos parametros criada, utilizando o LIKE ara #nao precisar ser identico

        if cultive is not None:
            query += " AND Cultive LIKE ?"
            params.append(f"%{cultive}%")

        if product is not None:
            query += " AND Product LIKE ?"
            params.append(f"%{product}%")
            
        if quant_min is not None and quant_min != "":
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) >= ?"
            params.append(quant_min)

        if quant_max is not None and quant_max != "":
            query += " AND CAST(REPLACE(Quantity_Kg, '.', '') AS REAL) <= ?"
            params.append(quant_max)
            
        
        cursor.execute(query, params)#monta a query e executa os filtros caso sejam passados
        rows = cursor.fetchall() #pega todos os dados da query

        data = [{"Year": row[0], "GroupName": row[1], "Cultive": row[2], "Quantity_Kg": row[3]} for row in rows]
        conn.close() #fecha conexao
        
        return {"success": True, "total": len(data), "data": data}


#rota da aba processamento, subseleção americanas e hibridas
@router.get("/processamento/viniferas/americanas&hibridas")
async def get_proc_ame_hib(
    year: int = Query(None, ge=1970, le=2023),
    group: str = Query(None),
    cultive: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None)
,
token_user: str = Depends(verifica_token)):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)

        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, GroupName, Cultive, Quantity_Kg FROM processamento WHERE 1=1" #query inicial
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
,
token_user: str = Depends(verifica_token)):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)

        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, GroupName, Cultive, Quantity_Kg FROM processamento WHERE 1=1" #query inicial
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
    

#rota da aba processamento, subseleção sem classificacao
@router.get("/processamento/viniferas/sem_classificacao")
async def get_proc_sem_class(
    year: int = Query(None, ge=1970, le=2023),
    group: str = Query(None),
    cultive: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None)
,
token_user: str = Depends(verifica_token)):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)

        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, SemClass, Cultive, Quantity_Kg FROM processamento WHERE 1=1" #query inicial
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
,
token_user: str = Depends(verifica_token)):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)

        conn = sqlite3.connect("vitibrasil.db")
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
,
token_user: str = Depends(verifica_token)):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM importacao WHERE 1=1" #query inicial
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

#product: Optional[str] = Query(None),  
#rota para aba de importacao de espumantes
@router.get("/importacao")
async def get_import_espumantes(
    year: int = Query(None, ge= 1970, le= 2024),
    country: str = Query(None),
    product: str = Query(None),
    quant_min: Optional[str] = Query(None),
    quant_max: Optional[str] = Query(None),
    value_min: Optional[str] = Query(None),
    value_max: Optional[str] = Query(None)
,
token_user: str = Depends(verifica_token)):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD, Product FROM importacao WHERE 1=1" #query inicial
        params = [] #lista dos filtros

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if country:
            query += " AND Country LIKE ?"
            params.append(country)
            
        if product:
            query += " AND Product LIKE ?"
            params.append(product)
        
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

        data = [{"Year": row[0], "Country": row[1], "Quantity_Kg": row[2], "Value_USD": row[3], "Product": row[4]} for row in rows]
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
,
token_user: str = Depends(verifica_token)):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM importacao WHERE 1=1" #query inicial
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
,
token_user: str = Depends(verifica_token)):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM importacao WHERE 1=1" #query inicial
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
,
token_user: str = Depends(verifica_token)):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM importacao WHERE 1=1" #query inicial
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
,
token_user: str = Depends(verifica_token)):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM exportacoes WHERE 1=1" #query inicial
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
,
token_user: str = Depends(verifica_token)):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM exportacoes WHERE 1=1" #query inicial
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
,
token_user: str = Depends(verifica_token)):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM exportacao WHERE 1=1" #query inicial
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
,
token_user: str = Depends(verifica_token)):
    try:
        quant_min_value = parse_float(quant_min)
        quant_max_value = parse_float(quant_max)
        value_max_quant = parse_float(value_max)
        value_min_quant = parse_float(value_min)
    
        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM exportacoes WHERE 1=1" #query inicial
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


