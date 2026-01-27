from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import verses, auth, devotionals
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from app import limiter

app = FastAPI(title="Bible API", description="API for accessing Bible verses and chapters")

# Add CORS middleware to allow cross-origin requests from specific origins
# origins = [
#     "http://localhost:3000",  # Example: Allow frontend on localhost:3000
#     "https://yourfrontenddomain.com",  # Example: Allow your deployed frontend
# ]

# Set up a rate limiter (using IP address as the key)

app.state.limiter = limiter

# Add SlowAPIMiddleware for global rate limiting
app.add_middleware(SlowAPIMiddleware)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

app.include_router(verses.router)
app.include_router(auth.router, prefix="/auth", tags=["auth"])
app.include_router(devotionals.router, tags=["devotionals"])

# Handle rate limit exceeded error
@app.exception_handler(RateLimitExceeded)
async def rate_limit_error(request, exc):
    return HTTPException(status_code=429, detail="Rate limit exceeded. Please try again later.")

# Use app.add_event_handler instead of on_event for startup and shutdown
async def startup():
    print("App starting up...")

async def shutdown():
    print("App shutting down...")

# Register event handlers explicitly
app.add_event_handler("startup", startup)
app.add_event_handler("shutdown", shutdown)