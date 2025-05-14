from fastapi import FastAPI
from routers import vitibrasil
import uvicorn

app = FastAPI(
    title="Vitibrasil API",
    version ="1.0.0"
    )

app.include_router(vitibrasil.router)

@app.get("/")
def root():
    return {"msg": "Vitibrasil API is aliiive"}

def main():
    uvicorn.run(app, host='127.0.0.1', port=8000)
    # uvicorn app:main --reload
        # para testar a api precisa ativar com o comando acima
    
if __name__ == "__main__":
    main()


