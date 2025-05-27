# 🧠 Projeto TechChallange - Fase 1

## ✅ Sobre

Este projeto busca retornar dados do site Embrapa, como por exemplo produção/importação de produtos como Espumantes, Vinho Tinto, em forma de API para ser usado futuramente em um app

---

## ⚙️ Stack utilizada

- **Linguagem:** Python 3.12
- **Framework Web:**  FastAPI
- **Servidor:**  Uvicorn
- **Gerenciador de pacotes:**  pip + requirements.txt
- **Testes:**  pytest (opcional)
- **Scraping:**  BeautifulSoup, requests
- **Banco de dados:**  SQLite (via sqlite3)
- **DataFrame:**  pandas
- **Validação:**  Pydantic
- **Autenticação:**  python-jose, passlib[bcrypt]
- **Outros** : openpyxl, lxml, bcrypt, certifi, charset-normalizer, click, ecdsa, et_xmlfile, anyio, annotated-types
---

## 🚀 Como rodar localmente

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
uvicorn app.main:app --reload
```

#### 4. Use as rotas
No navegador, acesse o URL/docs para ver quais APIs disponíveis