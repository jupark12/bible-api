from fastapi import APIRouter, HTTPException, status, Depends, Request
from fastapi.responses import JSONResponse
from app.utils import hash_password, verify_password, create_access_token
from app.models import UserCreate, UserInDB, User, UserLogin, TokenData
from app.database import db_connection
from datetime import timedelta
from app.config import SECRET_KEY, ALGORITHM
from app import limiter
from app.utils import get_current_user_from_cookie

# --- Define your existing router ---
router = APIRouter()

# --- Existing /register endpoint ---
@router.post("/register", response_model=User)
@limiter.limit("5/minute")
async def register_user(request: Request, user: UserCreate):
    async with db_connection() as conn:
        existing_user = await conn.fetchrow("SELECT * FROM users WHERE username = $1 OR email = $2", user.username, user.email)
        if existing_user:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username or email already registered")

        hashed_password = hash_password(user.password)

        # --- IMPORTANT: Ensure you insert first_name and last_name ---
        # Your UserCreate model expects them, and login/me endpoints need them.
        await conn.execute(
            """
            INSERT INTO users(username, email, password_hash, first_name, last_name, created_at)
            VALUES($1, $2, $3, $4, $5, NOW())
            """,
            user.username, user.email, hashed_password, user.first_name, user.last_name
        )
        # Return data consistent with the User response model
        return User(**user.model_dump()) # Use model_dump() for Pydantic v2+


# --- Existing /login endpoint ---
# Consider returning user data directly instead of just a message if needed by frontend immediately
@router.post("/login")
@limiter.limit("10/minute")
async def login_user(request: Request, credentials: UserLogin): # Changed type hint to UserLogin
    async with db_connection() as conn:
        # Fetch user by username only is usually sufficient for login if username is unique
        db_user_data = await conn.fetchrow("SELECT * FROM users WHERE username = $1", credentials.username)

        if not db_user_data or not verify_password(credentials.password, db_user_data['password_hash']):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, # Use status constants
                detail="Invalid username or password"
            )

        # Use the username confirmed from the database for the token subject
        access_token = create_access_token(data={"sub": db_user_data['username']})

        # Create response content *before* creating the JSONResponse
        response_content = {
            "message": "Login successful",
            "first_name": db_user_data.get('first_name'), # Use .get() for safety
            "last_name": db_user_data.get('last_name')
        }
        response = JSONResponse(content=response_content)

        # Set the cookie
        # For local dev over HTTP, set secure=False. Set secure=True for production HTTPS.
        response.set_cookie(
            key="access_token",
            value=access_token,
            httponly=True,      # Prevents JS access
            secure=False,       # Set to True if ONLY using HTTPS
            samesite='lax'     # Good default ('strict' can be too restrictive)
            # max_age=...      # Optionally set expiration same as JWT
            # path='/'         # Make cookie available site-wide
        )

        return response


# --- NEW /users/me endpoint ---
# Note: Added under the /auth prefix here, adjust prefix/router if needed
@router.get("/users/me", response_model=User)
@limiter.limit("10/minute")
async def read_users_me(request: Request, current_user: UserInDB = Depends(get_current_user_from_cookie)):
    """
    Get profile information for the currently authenticated user (via cookie).
    """
    # The dependency handles authentication and fetching the user.
    # The response_model=User ensures only safe fields (defined in User) are returned.
    # FastAPI automatically converts the UserInDB object from the dependency
    # to the User response model based on matching field names.
    return current_user

@router.post("/logout")
@limiter.limit("10/minute")
async def logout_user(request: Request, response: JSONResponse = JSONResponse(content={"message": "Logout successful"})):
    """
    Logs the user out by clearing the access_token cookie.
    """
    print("Attempting to clear access_token cookie...") # Debugging line
    # Tell the browser to delete the cookie by setting its expiry to the past (Max-Age=0)
    # IMPORTANT: Ensure 'path' and 'domain' (if used) match how the cookie was set during login.
    response = JSONResponse(content={"message": "Logout successful"})
    response.delete_cookie(
        key="access_token",
        domain=None 
    )
    return response