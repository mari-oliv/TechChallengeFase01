from datetime import datetime, timedelta
from jose import JWSError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from typing import Optional

SECRET_KEY = "chave"
ALGORITHM = "HS256"
ACCESS_TOKEN_MINUTES = 30

oauth2 = OAuth2PasswordBearer(tokenUrl="/token")

def cria_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"epx": expire})
    encode_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=[ALGORITHM])
    return encode_jwt

def verifica_token(token: str)