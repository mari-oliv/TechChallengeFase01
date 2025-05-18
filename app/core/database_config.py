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


def save_data(df: pd.DataFrame, table_db: str) -> None:
    logging.info('saving data')
    if not os.path.exists("vitibrasil.db"):
        logging.info('db not exist')
        init_db()
    
    conn = sqlite3.connect("vitibrasil.db")
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_db} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Year INTEGER,
            Country TEXT,
            Quantity_Kg TEXT, 
            Value_USD TEXT,
            Product TEXT,
            Page TEXT
        )
    ''')
    df.to_sql(table_db, conn, if_exists="append", index=False)
    conn.commit()
    conn.close()
    print("All data saved")
