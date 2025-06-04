# 🧠 Projeto TechChallange - Fase 1

## ✅ Sobre

Este projeto busca retornar dados do site Embrapa, como por exemplo produção/importação de produtos como Espumantes, Vinho Tinto, em forma de API para ser usado futuramente em um app

---

## ⚙️ Stack utilizada

- **Linguagem:** Python 3.12
- **Framework Web:**  FastAPI
- **Servidor:**  gunicorn uvicorn / render https://techchallengefase1-1.onrender.com
- **Gerenciador de pacotes:**  pip + requirements.txt
- **Testes:**  pytest (opcional)
- **Scraping:**  BeautifulSoup, requests
- **Banco de dados:**  SQLite (via sqlite3)
- **DataFrame:**  pandas
- **Validação:**  Pydantic
- **Autenticação:**  python-jose, passlib[bcrypt]
- **Outros** : openpyxl, lxml, bcrypt, certifi, charset-normalizer, click, ecdsa, et_xmlfile, anyio, annotated-types
---
## 🚀 Deploy

URL https://techchallengefase1-1.onrender.com

## APIs
https://techchallengefase1-1.onrender.com/docs

## Arquitetura do Projeto

![Main py (3)](https://github.com/user-attachments/assets/ee2a0665-2577-4575-b62e-8d1068f045f0)



## Como rodar localmente

#### 1. Clone o repositório
No terminal, execute os comandos:
```bash
git clone https://github.com/pecosta23/TechChallengeFase1
cd TechChallengeFase1
```

#### 2. Realize o Scraper para salvar no banco (Opcional - como fallback caso o site Vitibrasil esteja fora do ar)
No terminal, execute os comandos:
```bash
    python -m app.services.scraper_producao
    python -m app.services.scraper_exportacao
    python -m app.services.scraper_importacao
    python -m app.services.scraper_processamento
    python -m app.services.scraper_comercializacao
```

#### 3. Execute o servidor localmente
Acesse a pasta app/ e rode no terminal o uvicorn
```bash
cd TechChallengeFase1/app
gunicorn -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:10000s
```

#### 4. Use as rotas
No navegador, acesse o URL/docs para ver quais APIs disponíveis

