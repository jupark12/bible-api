from fastapi import APIRouter, HTTPException, Query, Request, Depends
from app.crud import get_verses_by_book_and_chapter, search_bible_text
from typing import List, Optional
from pydantic import BaseModel
from app import limiter
from app.crud import get_current_devotional, save_current_devotional
from app.utils import get_current_user_from_cookie
from app.models import User
from datetime import date
import datetime

class Devotional(BaseModel):
    devotional_id: int
    user_id: int
    devotional_date: date
    reflection: str
    created_at: datetime.datetime
    updated_at: datetime.datetime

router = APIRouter()

@router.get("/devotionals/today", response_model=Optional[Devotional], summary="Get the current user's devotional for today")
@limiter.limit("50/minute")
async def get_today_devotionals(request: Request, current_user: User = Depends(get_current_user_from_cookie)):
    """
    Retrieves the devotional entry saved by the currently authenticated user
    for today's date.

    Returns the devotional entry if found, otherwise returns null.
    """
    today_date: date = date.today()
    
    try:
        # You'll likely need to pass your database connection/session here
        # e.g., db=Depends(get_db) in function signature and pass db to CRUD
        devotional = await get_current_devotional(
            user_id=current_user.user_id, # Assuming your User model has user_id
            devotional_date=today_date
            # Pass db connection/session if required by your CRUD function
            # db=db_session
        )
    except Exception as e:
        # Log the error e
        print(f"Database error fetching devotional: {e}") # Replace with proper logging
        raise HTTPException(status_code=500, detail="Error retrieving devotional data.")

    if not devotional:
        # It's perfectly normal not to have a devotional for today yet.
        # Returning None is handled correctly by FastAPI with Optional[Model]
        return None

    return devotional # FastAPI will serialize this using the DevotionalEntry model

@router.get("/devotionals/save", response_model=Devotional)
@limiter.limit("50/minute")
async def save_devotional(request: Request, current_user: User = Depends(get_current_user_from_cookie)):
    """
    Saves a devotional entry for the currently authenticated user for today's date.

    Returns the saved devotional entry.
    """
    today_date: date = date.today()
    reflection: str = request.query_params.get("reflection")

    try:
        # You'll likely need to pass your database connection/session here
        # e.g., db=Depends(get_db) in function signature and pass db to CRUD
        devotional = await save_current_devotional(
            user_id=current_user.user_id, # Assuming your User model has user_id
            reflection=reflection
        )
    except Exception as e:
        # Log the error e
        print(f"Database error saving devotional: {e}") # Replace with proper logging
        raise HTTPException(status_code=500, detail="Error saving devotional data.")

    return devotional # FastAPI will serialize this using the DevotionalEntry model