from fastapi import FastAPI
from app.routers import vitibrasil
from app.core import init_db
import uvicorn
import logging

app = FastAPI(
    title="Vitibrasil API",
    version ="1.0.0"
    )

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message).200s",
    datefmt='%m/%d/%Y %I:%M:%S %p',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(".logs")
    ]
)

app.include_router(vitibrasil.router)

def main():
    uvicorn.run(app, host='127.0.0.1', port=8000)
    # uvicorn app:main --reload
        # para testar a api precisa ativar com o comando acima
    
if __name__ == "__main__":
    main()


