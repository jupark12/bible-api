# Bible API

A RESTful API built with FastAPI for accessing Bible verses, managing user accounts, and tracking daily devotionals. This API provides full-text search capabilities, user authentication, and personal Bible study features.

## Features

- 📖 **Bible Verse Access** - Retrieve verses by book, chapter, and verse number
- 🔍 **Full-Text Search** - Search across all 31,102 verses using PostgreSQL's powerful search
- 👤 **User Authentication** - JWT-based authentication with secure cookie sessions
- 📝 **Daily Devotionals** - Create and manage personal devotional entries with favorite verses
- 📊 **Progress Tracking** - Track Bible reading progress and statistics
- 🛡️ **Rate Limiting** - Built-in rate limiting to prevent API abuse
- 🌐 **CORS Support** - Configured for cross-origin requests

## Tech Stack

- **Framework:** FastAPI (Python 3.8+)
- **Database:** PostgreSQL 14+
- **Authentication:** JWT tokens with httponly cookies
- **ORM/Driver:** asyncpg (async) + psycopg2 (import script)
- **Password Hashing:** bcrypt via passlib
- **Rate Limiting:** SlowAPI

## Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.8 or higher** - [Download Python](https://www.python.org/downloads/)
- **PostgreSQL 14 or higher** - [Download PostgreSQL](https://www.postgresql.org/download/)
- **Homebrew** (for macOS) - [Install Homebrew](https://brew.sh/)

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd bible-api
```

### 2. Install PostgreSQL

**macOS:**
```bash
brew install postgresql@16
brew services start postgresql@16
```

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
```

**Windows:**
Download and install from [postgresql.org](https://www.postgresql.org/download/windows/)

### 3. Create Database

```bash
# macOS/Linux (replace 'yourusername' with your system username)
createdb -U yourusername bible_app

# Or using psql
psql -U postgres
CREATE DATABASE bible_app;
\q
```

### 4. Set Up Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # macOS/Linux
# OR
venv\Scripts\activate  # Windows
```

### 5. Install Python Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 6. Configure Environment Variables

Create a `.env` file in the project root:

```bash
# .env
DATABASE_URL=postgresql://yourusername:yourpassword@localhost:5432/bible_app
SECRET_KEY=your-secret-key-change-this-in-production
```

**Notes:**
- Replace `yourusername` with your PostgreSQL username (often your system username)
- Replace `yourpassword` with your PostgreSQL password (leave blank if no password)
- Generate a secure SECRET_KEY for production (e.g., using `openssl rand -hex 32`)

### 7. Update Import Script Configuration

Edit `bible-data/importBible.py` and update lines 7-14 with your database credentials:

```python
DB_NAME = "bible_app"
DB_USER = "yourusername"  # Your PostgreSQL username
DB_PASSWORD = ""          # Your PostgreSQL password
DB_HOST = "localhost"
DB_PORT = "5432"

# Update CSV path to match your project location
CSV_FILE_PATH = "/full/path/to/bible-api/bible-data/asv/asv.csv"
```

**Make sure lines 297-304 are uncommented** to enable table creation and data import:

```python
# Create tables
create_tables(conn)

# Import data from the CSV file
if import_from_csv(conn, CSV_FILE_PATH):
    print("Bible import completed successfully!")
```

### 8. Import Bible Data

```bash
python bible-data/importBible.py
```

Expected output:
```
Processing CSV file...
Successfully imported Bible data from ...
Imported 66 books, 1189 chapters, and 31102 verses
Functions created successfully!
```

### 9. Run the Application

```bash
uvicorn app.main:app --reload
```

The API will be available at:
- **API Base URL:** http://localhost:8000
- **Interactive Docs:** http://localhost:8000/docs
- **Alternative Docs:** http://localhost:8000/redoc

## API Endpoints

### Bible Verses

- `GET /verses/{book_name}/{chapter_number}` - Get all verses in a chapter
  - Example: `/verses/Genesis/1`
  
- `GET /verses/{book_name}/{chapter_number}/{verse_start}/{verse_end}` - Get verse range
  - Example: `/verses/John/3/16/17`

- `GET /search?query={text}&limit={number}` - Search Bible text
  - Example: `/search?query=love&limit=10`

### Authentication

- `POST /auth/register` - Register new user
- `POST /auth/login` - Login (sets httponly cookie)
- `POST /auth/logout` - Logout (clears cookie)
- `GET /auth/users/me` - Get current user info (requires authentication)

### Devotionals (Requires Authentication)

- `GET /devotionals` - Get all devotionals for user
  - Query params: `limit`, `offset`, `order_by`
  
- `GET /devotionals/today` - Get today's devotional
  
- `POST /devotionals/save` - Save/update devotional entry

## Usage Examples

### Fetch a Chapter

```bash
curl http://localhost:8000/verses/Genesis/1
```

### Fetch Specific Verses

```bash
curl http://localhost:8000/verses/John/3/16/16
```

### Search the Bible

```bash
curl "http://localhost:8000/search?query=faith&limit=5"
```

### Register a User

```bash
curl -X POST http://localhost:8000/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "username": "john",
    "email": "john@example.com",
    "password": "securepassword",
    "first_name": "John",
    "last_name": "Doe"
  }'
```

### Login

```bash
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "username": "john",
    "password": "securepassword"
  }' \
  -c cookies.txt
```

## Database Schema

The database includes the following main tables:

- **books** - 66 Bible books (Genesis, Exodus, etc.)
- **chapters** - 1,189 chapters
- **verses** - 31,102 verses with full text
- **users** - User accounts with hashed passwords
- **devotionals** - Daily devotional entries
- **favorite_verses** - User's favorite verses
- **reading_plans** - Reading plan definitions
- **user_reading_plans** - User's active reading plans
- **reading_sessions** - Reading session tracking

## Rate Limits

- Most endpoints: 100 requests/minute per IP
- Verse endpoints: 150 requests/minute per IP
- Search endpoint: 50 requests/minute per IP
- Auth endpoints: 5-10 requests/minute per IP

## Development

### Running Tests

```bash
pytest
```

### Checking Database

```bash
# Connect to database
psql -U yourusername -d bible_app

# List tables
\dt

# Check verse count
SELECT COUNT(*) FROM verses;

# Exit
\q
```

## Troubleshooting

### "ModuleNotFoundError: No module named 'X'"

**Solution:** Make sure virtual environment is activated and dependencies are installed:
```bash
source venv/bin/activate
pip install -r requirements.txt
```

### "Connection refused" or "Role does not exist"

**Solution:** Check your `.env` file has correct database credentials:
```bash
DATABASE_URL=postgresql://correct_username:correct_password@localhost:5432/bible_app
```

### "Verses not found" errors

**Solution:** Database might be empty. Run the import script:
```bash
python bible-data/importBible.py
```

### Port 8000 already in use

**Solution:** Either stop the other process or use a different port:
```bash
uvicorn app.main:app --reload --port 8001
```

### PostgreSQL not running

**macOS:**
```bash
brew services start postgresql@16
```

**Linux:**
```bash
sudo systemctl start postgresql
```

## Project Structure

```
bible-api/
├── app/
│   ├── __init__.py          # Rate limiter initialization
│   ├── main.py              # FastAPI application entry point
│   ├── config.py            # Configuration settings
│   ├── database.py          # Database connection management
│   ├── models.py            # Pydantic data models
│   ├── crud.py              # Database operations
│   ├── utils.py             # Helper functions (JWT, hashing)
│   └── routes/
│       ├── __init__.py
│       ├── verses.py        # Bible verse endpoints
│       ├── auth.py          # Authentication endpoints
│       └── devotionals.py   # Devotional endpoints
├── bible-data/
│   ├── importBible.py       # Database setup and data import script
│   └── asv/
│       └── asv.csv          # American Standard Version Bible text
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables (create this)
└── README.md               # This file
```

## Bible Translation

This API currently includes the **American Standard Version (ASV)** translation with 31,102 verses covering all 66 books of the Bible.

## License

[Add your license here]

## Contributing

[Add contributing guidelines here]

## Support

For issues or questions, please [open an issue](link-to-issues) or contact [your-email].