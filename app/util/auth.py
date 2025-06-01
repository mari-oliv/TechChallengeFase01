from datetime import datetime, timedelta
from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from typing import Optional
from passlib.context import CryptContext
from bcrypt import hashpw, gensalt, checkpw

SECRET_KEY = "chave"
ALGORITHM = "HS256"
ACCESS_TOKEN_MINUTES = 30

oauth2 = OAuth2PasswordBearer(tokenUrl="/login")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

#define
def hash_pass(password: str) -> str:
    return pwd_context.hash(password)

#verifies
def verifica_pass(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def cria_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"epx": expire.timestamp()})
    encode_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encode_jwt

def verifica_token(token: str = Depends(oauth2)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="O token não é válido")
        return username
    except JWTError:
        raise HTTPException(status_code=401, detail="O token não é válido")

