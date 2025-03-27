from pydantic import BaseModel
from datetime import datetime

class UserInDB(BaseModel):
    username: str
    email: str
    password_hash: str
    created_at: datetime

class User(BaseModel):
    username: str
    email: str

class UserCreate(BaseModel):
    username: str
    email: str
    password: str
