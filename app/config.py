# app/config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the database URL from environment variable or use a default value
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/bible_db")

# You can add more configurations if needed, e.g., for security, CORS, or JWT settings

