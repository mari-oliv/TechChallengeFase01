from fastapi import APIRouter, Query, Depends, HTTPException, Form, Body
from typing import Optional
import sqlite3
from app.util.auth import verifica_token, cria_token, hash_pass, verifica_pass
from app.core.database_config import init_db
from app.core.logging_config import logging_config
import logging
from pydantic import BaseModel
from app.services.scraper_producao import get_producao
from app.services.scraper_processamento import get_processamento
from app.services.scraper_comercializacao import get_comercializacao
from app.services.scraper_importacao import get_importacao
from app.services.scraper_exportacao import get_exportacao
from fastapi.responses import JSONResponse

router = APIRouter()
logging_config()
class UserRequest(BaseModel):
    username: str
    password: str

@router.get("/", responses={
        200: {
            "description": "Dados de produção retornados com sucesso.",
            "content": {
                "application/json": {
                    "example": {
                        "message": "Vitibrasil API is aliiive"
                    }
                }
            }
        },
        422: {
            "description": "Erro de validação dos parâmetros.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "year"],
                                "msg": "value is not a valid integer",
                                "type": "type_error.integer"
                            }
                        ]
                    }
                }
            }
        }
    })
async def root() -> dict:
    """
        ### Descrição:
            Rota raiz da API Vitibrasil.
        ### Parâmetros:
            None.
        ### Retorno:
            Retorna uma mensagem de confirmação de que a API está funcionando.
    """
    return JSONResponse(content={"message": "Vitibrasil API is aliiive"})

@router.post(
    "/signup",
    responses={
        200: {
            "description": "Usuário cadastrado com sucesso.",
            "content": {
                "application/json": {
                    "example": {
                        "message": "Usuário cadastrado com sucesso!"
                    }
                }
            }
        },
        422: {
            "description": "Erro de validação dos dados.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["body", "username"],
                                "msg": "field required",
                                "type": "value_error.missing"
                            }
                        ]
                    }
                }
            }
        }
    }
)
async def signup(user: UserRequest = Body(
        ...,
        example={
            "username": "usuario_exemplo",
            "password": "senha_segura"
        }
    )
) -> dict:
    """
        ### Descrição:
            Rota de cadastro de usuários.
       ### Parâmetros:
            - method: POST
            - headers: content-type: application/json
            - Body JSON:
                {
                    "username": "user",
                    "password": "pass"
                }
        ### Retorno:
            Retorna uma mensagem de confirmação de que o usuário foi cadastrado com sucesso.
    """
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
        return JSONResponse(status_code=200,content={"message": "Usuário cadastrado com sucesso!"})
    except sqlite3.IntegrityError:
        logging.error(f"Usuário {user.username} já existe.")
        raise HTTPException(status_code=202, detail="Usuário já existe.")
    finally:
        conn.close()

@router.post(
    "/token",
    responses={
        200: {
            "description": "Token de acesso gerado com sucesso.",
            "content": {
                "application/json": {
                    "example": {
                        "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                        "token_type": "bearer"
                    }
                }
            }
        },
        422: {
            "description": "Erro de validação dos dados.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["body", "username"],
                                "msg": "field required",
                                "type": "value_error.missing"
                            }
                        ]
                    }
                }
            }
        }
    }
)
async def login_user(user: UserRequest = Body(
        ...,
        example={
            "username": "usuario_exemplo",
            "password": "senha_segura"
        }
    )
) -> dict:
    """
        ### Descrição:
            Esta rota é responsável por autenticar o usuário e retornar um token de acesso.
       ### Parâmetros:
            - headers: content-type: application/json
            - method: POST
            - Body JSON:
                {
                    "username": "user",
                    "password": "pass"
                }
        ### Retorno:
            Retorna o token de acesso se as credenciais forem válidas.
    """
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()

    cursor.execute("SELECT password FROM users WHERE username = ?", (user.username,))
    result = cursor.fetchone()
    conn.close()

    if not result or not verifica_pass(user.password, result[0]):
        raise HTTPException(status_code=401, detail="As credenciais sao invalidas")

    access_token = cria_token(data={"sub": user.username})
    return JSONResponse(status_code=200, content={"access_token": access_token, "token_type": "bearer"})

@router.get(
    "/producao",
    responses={
        200: {
            "description": "Dados de produção retornados com sucesso.",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "total": 1,
                        "data": [
                            {
                                "Year": 2020,
                                "Product": "Uva",
                                "Category": "Vinho",
                                "Quantity_L": 123456
                            }
                        ]
                    }
                }
            }
        },
        422: {
            "description": "Erro de validação dos parâmetros.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "year"],
                                "msg": "value is not a valid integer",
                                "type": "type_error.integer"
                            }
                        ]
                    }
                }
            }
        }
    }
)
async def producao(
    year: int = Query(None, ge=1970, le=2023),
    product: Optional[str] = Query(None),  
    category: Optional[str] = Query(None),
    token_user: str = Depends(verifica_token)
) -> dict:
    """
        ### Descrição:
            Rota de Produção.
       ### Parâmetros:
            - headers:
                - Authorization: Bearer {token}
            - method: GET
            - parameters:
                - year: int (obrigatório, ano de 1970 a 2023)
                - product: str (opcional, nome do produto)
                - category: str (opcional, categoria do produto)
        ### Retorno:
            Retorna dados de produção filtrados por ano, produto e categoria.
        ### Exemplo de uso:
            GET /producao?year=2020&product=uva&category=vinho
            Retorna dados de produção de uva para o ano de 2020 na categoria vinho.
    """
    try:
        df = get_producao(year)
        data = df.to_dict(orient="records")
        logging.info("Dados do site coletados com sucesso")
        filtered_data = data
        
        if product:
            filtered_data = [row for row in filtered_data if row.get("Product") and product.lower() in row["Product"].lower()]
        if category:
            filtered_data = [row for row in filtered_data if row.get("Category") and category.lower() in row["Category"].lower()]
        return JSONResponse(status_code=200, content={"success": True, "total": len(filtered_data), "data": filtered_data})
    except Exception as e:
        logging.error(f"Erro ao capturar dados do banco: {e}")
        return JSONResponse(status_code=500, content={"Success": False, "error": str(e)})
    except:
        logging.info("Erro ao capturar dados do site, tentando coletar do banco")
        conn = sqlite3.connect("vitibrasil.db") 
        cursor = conn.cursor()

        if year and product:
            logging.info(f"Capturando dados do banco para o ano {year} e produto {product}")
            query = "SELECT Year, Product, Quantity_L FROM producao WHERE Year = ? AND Product LIKE ?"
            cursor.execute(query, (year, product))

        else:
            query = "SELECT Year, Product, Quantity_L FROM producao WHERE Year = ?"
            cursor.execute(query,(year,))

        rows = cursor.fetchall()

        if not rows:
            logging.warning("Consulta ao banco realizada, mas nenhum dado encontrado.")
            conn.close()
            return JSONResponse(status_code=200, content={"success": True, "total": 0, "data": [], "message": "Nenhum dado encontrado no banco para os filtros informados."})

        data = [{"Year": row[0], "Product": row[1], "Quantity_L": row[2]} for row in rows]
        conn.close()
        return JSONResponse(status_code=200, content={"success": True, "total": len(data), "data": data})

    
@router.get("/processamento", responses={
        200: {
            "description": "Dados de processamento retornados com sucesso.",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "total": 1,
                        "data": [
                            {
                                "Year": 2020,
                                "GroupName": "Uva",
                                "Cultive": "Vinho",
                                "Quantity_Kg": 123456,
                                "Product": "Viníferas"
                            }
                        ]
                    }
                }
            }
        },
        422: {
            "description": "Erro de validação dos parâmetros.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "year"],
                                "msg": "value is not a valid integer",
                                "type": "type_error.integer"
                            }
                        ]
                    }
                }
            }
        }
    })
async def processamento(
    product: str = Query(None),
    year: int = Query(None, ge=1970, le=2023),
    group:  Optional[str] = Query(None),
    cultive:  Optional[str] = Query(None),
    token_user: str = Depends(verifica_token))  -> dict:
    """
        ### Descrição:
            Rota de Processamento.
       ### Parâmetros:
            - headers:
                - Authorization: Bearer {token}
            - method: GET
            - parameters:
                - year: int (obrigatório, ano de 1970 a 2023)
                - product: str (obrigatório, nome do produto)
                - cultive: str (opcional, cultivo do produto)
        ### Retorno:
            Retorna dados de produção filtrados por ano, produto e cultivo.
        ### Exemplo de uso:
            GET /processamento?year=2020&product=uva&cultive=Grand Noir
            Retorna dados de produção de uva para o ano de 2020 na categoria Grand Noir.
    """
    if product is None:
        return {"Necessário informar o produto": "Viníferas, Uvas de mesa, Americanas e Híbridas ou Sem Classificação"}
    
    if product == 'Viníferas':
        option = 1
    elif product == 'Americanas e Híbridas':
        option = 2
    elif product == 'Uvas de mesa':
        option = 3
    elif product == 'Sem Classificação':
        option = 4
    else:
        pass
    
    try:
        df = get_processamento(year, option)
   
        if group:
            df = df[df["GroupName"].str.contains(group, case=False, na=False)]
        if cultive:
            df = df[df["Cultive"].str.contains(cultive, case=False, na=False)]
        if product:
            df = df[df["Product"].str.contains(product, case=False, na=False)]
        data = df.to_dict(orient="records")
        logging.info("Dados do site coletados com sucesso")
        return JSONResponse(status_code=200, content={"success": True, "total": len(data), "data": data})
    except Exception as e:
        logging.error(f"Erro ao capturar dados do banco: {e}")
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})
    except:
        logging.error("Erro ao capturar dados do site, tentando coletar do banco")

        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, GroupName, Cultive, Quantity_Kg, Product FROM processamento WHERE 1=1" 
        params = [] 

        if year is not None:
            query += " AND Year = ?"
            params.append(year) 

        if group:
            query += " AND GroupName LIKE ?"
            params.append(f"%{group}%")

        if cultive:
            query += " AND Cultive LIKE ?"
            params.append(f"%{cultive}%")
            
        if product:
            query += " AND Product LIKE ?"
            params.append(f"%{product}%")

        if year is not None:
            query += " AND Year = ?"
            params.append(year) 
        
        logging.info("QUERY:", query)
        logging.info("PARAMS:", params)
        cursor.execute(query, params) 
        rows = cursor.fetchall() 

        data = [{"Year": row[0], "GroupName": row[1], "Cultive": row[2], "Quantity_Kg": row[3]} for row in rows]
        conn.close() 
        
        return JSONResponse(status_code=200, content={"success": True, "total": len(data), "data": data})
    

@router.get("/comercializacao/", responses={
        200: {
            "description": "Dados de comercialização retornados com sucesso.",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "total": 1,
                        "data": [
                            {
                                "Year": 2020,
                                "GroupName": "Uva",
                                "Cultive": "Vinho",
                                "Quantity": 123456
                            }
                        ]
                    }
                }
            }
        },
        422: {
            "description": "Erro de validação dos parâmetros.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "year"],
                                "msg": "value is not a valid integer",
                                "type": "type_error.integer"
                            }
                        ]
                    }
                }
            }
        }
    })
async def get_comercializacao(
    year: int = Query(None, ge=1970, le=2023),
    group: Optional[str] = Query(None),
    cultive: Optional[str] = Query(None),
    token_user: str = Depends(verifica_token))  -> dict:
    """
        ### Descrição:
            Rota de Comercialização.
       ### Parâmetros:
            - headers:
                - Authorization: Bearer {token}
            - method: GET
            - parameters:
                - year: int (obrigatório, ano de 1970 a 2023)
                - group: str (opcional, nome do grupo)
                - cultive: str (opcional, cultivo do produto)
        ### Retorno:
            Retorna dados de produção em JSON filtrados por ano, grupo e cultivo. 
        ### Exemplo de uso:
            GET /processamento?year=2020&group=uva&cultive=Grand Noir
            Retorna dados de produção de uva para o ano de 2020 na categoria Grand Noir.
    """
    try:
        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, GroupName, Cultive, Quantity_L FROM comercializacao WHERE 1=1" 
        params = []

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if group:
            query += " AND GroupName LIKE ?"
            params.append(group)
        
        if cultive:
            query += " AND Cultive LIKE ?"
            params.append(cultive)
    

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "GroupName": row[1], "Cultive": row[2], "Quantity": row[3]} for row in rows]
        conn.close()

        return JSONResponse(content={"success": True, "total": len(data), "data": data})
    
    except Exception as e:
        raise HTTPException(status_code=500, detail={"success": False, "error": str(e)})

@router.get("/importacao", responses={
        200: {
            "description": "Dados de importação retornados com sucesso.",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "total": 1,
                        "data": [
                            {
                                "Year": 2020,
                                "Country": "França",
                                "Quantity_Kg": 123456,
                                "Value_USD": 1000000,
                                "Product": "Vinhos de mesa"
                            }
                        ]
                    }
                }
            }
        },
        422: {
            "description": "Erro de validação dos parâmetros.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "year"],
                                "msg": "value is not a valid integer",
                                "type": "type_error.integer"
                            }
                        ]
                    }
                }
            }
        }
    })
async def importacao(
    year: int = Query(None, ge= 1970, le= 2024),
    country: Optional[str] = Query(None),
    product: str = Query(None),
    token_user: str = Depends(verifica_token))  -> dict:
    """
        ### Descrição:
            Rota de Importação.
       ### Parâmetros:
            - headers:
                - Authorization: Bearer {token}
            - method: GET
            - parameters:
                - year: int (obrigatório, ano de 1970 a 2023)
                - country: str (opcional, nome do país importador)
                - product: str (obrigatório, nome do produto)
        ### Retorno:
            Retorna dados de importação filtrados por ano, país e produto.
        ### Exemplo de uso:
            GET /importacao?year=2020&country=França&product=Vinhos de mesa
            Retorna dados de importação de Vinhos de mesa para o ano de 2020 da França.
    """
    if product is None:
        return {"Necessário informar o produto": "Vinhos de mesa, Espumantes, Uvas frescas, Uvas passas ou Suco de uva"}

    if product == 'Vinhos de mesa':
        option = 1
    elif product == 'Espumantes':
        option = 2
    elif product == 'Uvas frescas':
        option = 3
    elif product == 'Uvas passas':
        option = 4
    elif product == 'Suco de uva':
        option = 5
    else:
        pass

    try:
        df = get_importacao(year, option)
        data = df.to_dict(orient="records")
        logging.info("Dados do site coletados com sucesso")
        filtered_data = data
        if product:
            filtered_data = [row for row in filtered_data if row.get("Product") and product.lower() in row["Product"].lower()]
        if country:
            filtered_data = [row for row in filtered_data if row.get("Country") and country.lower() in row["Country"].lower()]
        return JSONResponse(status_code=200, content={"success": True, "total": len(filtered_data), "data": filtered_data})
    except Exception as e:
        logging.error(f"Erro ao capturar dados do banco: {e}")
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})
    except:
        logging.error("Erro ao capturar dados do site, tentando coletar do banco")
    
        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD, Product FROM importacao WHERE 1=1"
        params = []

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if country:
            query += " AND Country LIKE ?"
            params.append(country)
            
        if product:
            query += " AND Product LIKE ?"
            params.append(product)
        

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "Country": row[1], "Quantity_Kg": row[2], "Value_USD": row[3], "Product": row[4]} for row in rows]
        conn.close()

        return JSONResponse(status_code=200, content={"success": True, "total": len(data), "data": data})

@router.get("/exportacao", responses={
        200: {
            "description": "Dados de exportação retornados com sucesso.",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "total": 1,
                        "data": [
                            {
                                "Year": 2020,
                                "Country": "França",
                                "Quantity_Kg": 123456,
                                "Value_USD": 1000000,
                                "Product": "Vinhos de mesa"
                            }
                        ]
                    }
                }
            }
        },
        422: {
            "description": "Erro de validação dos parâmetros.",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "year"],
                                "msg": "value is not a valid integer",
                                "type": "type_error.integer"
                            }
                        ]
                    }
                }
            }
        }
    })
async def exportacao (
    year: int = Query(None, ge= 1970, le= 2024),
    product: str = Query(None),
    country: Optional[str] = Query(None),
    token_user: str = Depends(verifica_token))  -> dict:
    """
        ### Descrição:
            Rota de Exportação.
       ### Parâmetros:
            - headers:
                - Authorization: Bearer {token}
            - method: GET
            - parameters:
                - year: int (obrigatório, ano de 1970 a 2023)
                - country: str (opcional, nome do país exportador)
                - product: str (obrigatório, nome do produto)
        ### Retorno:
            Retorna dados de exportação filtrados por ano, país e produto.
        ### Exemplo de uso:
            GET /exportacao?year=2020&country=França&product=Vinhos de mesa
            Retorna dados de exportação de Vinhos de mesa para o ano de 2020 da França.
    """
    if product is None:
        return {"Necessário informar o produto": "Vinhos de mesa, Espumantes, Uvas frescas ou Suco de uva"}

    if product == 'Vinhos de mesa':
        option = 1
    elif product == 'Espumantes':
        option = 2
    elif product == 'Uvas frescas':
        option = 3
    elif product == 'Suco de uva':
        option = 4
    else:
        pass
    
    try:
        df = get_exportacao(year, option)
        if country:
            df = df[df["Country"].str.contains(country, case=False, na=False)]
        if product:
            df = df[df["Product"].str.contains(product, case=False, na=False)]
        data = df.to_dict(orient="records")
        logging.info("Dados do site coletados com sucesso")
        return JSONResponse(status_code=200, content={"success": True, "total": len(data), "data": data})
    except Exception as e:
        logging.error(f"Erro ao capturar dados do banco: {e}")
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})
    except:
        logging.error("Erro ao capturar dados do site, tentando coletar do banco")

        conn = sqlite3.connect("vitibrasil.db")
        cursor = conn.cursor()

        query = "SELECT Year, Country, Quantity_Kg, Value_USD FROM exportacoes WHERE 1=1" 
        params = [] 

        if year is not None:
            query += " AND Year = ?"
            params.append(year)
        
        if country:
            query += " AND Country LIKE ?"
            params.append(country)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        data = [{"Year": row[0], "Country": row[1], "Quantity_Kg": row[2], "Value_USD": row[3]} for row in rows]
        conn.close()

        return JSONResponse(status_code=200, content={"success": True, "total": len(data), "data": data})

