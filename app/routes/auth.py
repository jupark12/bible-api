from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.responses import JSONResponse
from app.utils import hash_password, verify_password, create_access_token
from app.models import UserCreate, UserInDB, User
from app.database import db_connection
from datetime import timedelta

router = APIRouter()

@router.post("/register", response_model=User)
async def register_user(user: UserCreate):
    async with db_connection() as conn:
        # Check if the username or email already exists
        existing_user = await conn.fetchrow("SELECT * FROM users WHERE username = $1 OR email = $2", user.username, user.email)
        if existing_user:
            raise HTTPException(status_code=400, detail="Username or email already registered")
        
        hashed_password = hash_password(user.password)
        await conn.execute(
            "INSERT INTO users(username, email, password_hash, created_at) VALUES($1, $2, $3, NOW())",
            user.username, user.email, hashed_password
        )
        
        return {"username": user.username, "email": user.email}

@router.post("/login")
async def login_user(user: UserCreate):
    async with db_connection() as conn:
        # Fetch the user by username or email
        db_user = await conn.fetchrow("SELECT * FROM users WHERE username = $1 OR email = $2", user.username, user.email)
        if not db_user or not verify_password(user.password, db_user['password_hash']):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        
        access_token = create_access_token(data={"sub": user.username})
        
        response = JSONResponse(content={"message": "Login successful"})
        response.set_cookie(key="access_token", value=access_token, httponly=True, secure=True)
        
        return response