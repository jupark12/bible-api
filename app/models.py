from pydantic import BaseModel
from datetime import datetime
from datetime import date
from typing import List, Optional


class UserInDB(BaseModel):
    user_id: int
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


class FavoriteVerse(BaseModel):
    verse_id: int
    text: str
    book_name: str
    chapter_number: int
    verse_number: int


class Devotional(BaseModel):
    devotional_id: int
    user_id: int
    devotional_date: date
    reflection: str
    favorite_verses: Optional[List[FavoriteVerse]] = []
    created_at: datetime
    updated_at: datetime
