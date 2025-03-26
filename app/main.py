from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import verses

app = FastAPI(title="Bible API", description="API for accessing Bible verses and chapters")

# Add CORS middleware to allow cross-origin requests from specific origins
# origins = [
#     "http://localhost:3000",  # Example: Allow frontend on localhost:3000
#     "https://yourfrontenddomain.com",  # Example: Allow your deployed frontend
# ]

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

app.include_router(verses.router)

# Use app.add_event_handler instead of on_event for startup and shutdown
async def startup():
    print("App starting up...")

async def shutdown():
    print("App shutting down...")

# Register event handlers explicitly
app.add_event_handler("startup", startup)
app.add_event_handler("shutdown", shutdown)