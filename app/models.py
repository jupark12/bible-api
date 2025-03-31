from pydantic import BaseModel
from datetime import datetime

class UserInDB(BaseModel):
    username: str
    email: str
    first_name: str
    last_name: str
    password_hash: str
    created_at: datetime

class User(BaseModel):
    username: str
    email: str
    first_name: str
    last_name: str

class UserCreate(BaseModel):
    username: str
    email: str
    password: str
    first_name: str
    last_name: str

class UserLogin(BaseModel):
    username: str
    password: str

class TokenData(BaseModel):
    username: str | None = None

