import asyncpg
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user@localhost:5432/bible_app")

# Function to get a database connection
async def get_db_connection():
    return await asyncpg.connect(DATABASE_URL)

# Context manager for database connections
@asynccontextmanager
async def db_connection():
    conn = await get_db_connection()
    try:
        yield conn
    finally:
        await conn.close()