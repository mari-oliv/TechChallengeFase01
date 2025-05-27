import logging
import nest_asyncio
import uvicorn
from app.core import init_db
from app.routers import vitibrasil
from fastapi import FastAPI

nest_asyncio.apply()

app = FastAPI(
    title="Vitibrasil API",
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
    # await init_db()
    # uvicorn.run(app, host='127.0.0.1', port=5000)
    
if __name__ == "__main__":
    main()
