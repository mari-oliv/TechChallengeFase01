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


def save_at_db_importacao(df: pd.DataFrame) -> None:
    conn = sqlite3.connect("vitibrasil_import.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS importacao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Year INTEGER,
            Country TEXT,
            Quantity_Kg TEXT, 
            Value_USD TEXT,
            Product TEXT
        )
    ''')#cria a table

    df.to_sql("importacao", conn, if_exists="append", index=False)

    conn.commit()
    conn.close()
    print("All data saved")