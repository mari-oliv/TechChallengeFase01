import logging
import nest_asyncio
import uvicorn
from app.core import init_db
from app.routers import vitibrasil
from fastapi import FastAPI
import gunicorn

nest_asyncio.apply()

app = FastAPI(
    title="Vitivinicultura API",
    description="TechChallenge Fase 1 - Machine Learning Engineering na FIAP. API desenvolvido para fornecer informações sobre da Vitivinicultura do site Vitibrasil.",
    contact={
        "name": "Pedro Costa e Marina Oliveira",
        "url": "https://github.com/pecosta23/TechChallengeFase1"
    },
    version ="1.0.0"
)


logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message).200s",
    datefmt='%d/%m/%Y %I:%M:%S %p',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app/logs/app.log")
    ]
)

app.include_router(vitibrasil.router)

async def main():
    logging.info("Starting Vitibrasil API...")
    await init_db()

if __name__ == "__main__":
    main()
