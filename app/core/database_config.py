import sqlite3
import os
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message).200s",
    datefmt='%m/%d/%Y %I:%M:%S %p',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(".logs")
    ]
)

async def init_db():
    if not os.path.exists("vitibrasil.db"):
        logging.info("Banco de dados não encontrado. Criando...")
        conn = sqlite3.connect("vitibrasil.db")
        logging.info("Banco de dados criado com sucesso.")
    else:
        logging.info("Banco de dados já existe.")
        
    if not os.path.exists("users.db"):
        logging.info("Banco de dados não encontrado. Criando...")
        conn = sqlite3.connect("users.db")
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE,
                password TEXT
            )
        ''')
        conn.commit()
        conn.close()
        logging.info("Banco de dados criado com sucesso.")
    else:
        logging.info("Banco de dados já existe.")