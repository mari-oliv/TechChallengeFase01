from fastapi import FastAPI
from app.routers import vitibrasil

app = FastAPI(
    title="Vitibrasil API",
    version ="1.0.0"
    )

app.include_router(vitibrasil.router)

@app.get("/")
def root():
    return {"msg": "Vitibrasil API is aliiive"}



