from fastapi import Request, HTTPException, status
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
from app.config import SECRET_KEY, ALGORITHM
from app.models import TokenData, UserInDB
from app.database import db_connection
from pydantic import ValidationError

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict, expires_delta: timedelta = timedelta(hours=1)) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# --- Add the dependency function get_current_user_from_cookie (as shown above) ---
async def get_current_user_from_cookie(request: Request):
    # ... (dependency code as above) ...
    token = request.cookies.get("access_token")
    print(token)
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
    )
    if token is None: raise credentials_exception
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str | None = payload.get("sub")
        if username is None: raise credentials_exception
        token_data = TokenData(username=username)
        print(token_data.username)
    except (JWTError, ValidationError) as e:
        raise credentials_exception
    async with db_connection() as conn:
        db_user_data = await conn.fetchrow("SELECT * FROM users WHERE username = $1", token_data.username)
    if db_user_data is None: raise credentials_exception
    try:
        print(db_user_data)
        user_in_db = UserInDB(**db_user_data)
    except ValidationError as e:
        raise HTTPException(status_code=500, detail="Error processing user data")
    return user_in_db