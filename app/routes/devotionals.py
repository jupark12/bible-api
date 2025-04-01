from fastapi import APIRouter, HTTPException, Query, Request, Depends
from app.crud import get_verses_by_book_and_chapter, search_bible_text
from typing import List, Optional
from pydantic import BaseModel, Field
from app import limiter
from app.crud import get_current_devotional, save_current_devotional, get_all_devotionals
from app.utils import get_current_user_from_cookie
from app.models import User, FavoriteVerse, Devotional
from datetime import date
import datetime


class DevotionalSavePayload(BaseModel):
    reflection: str = Field(..., min_length=1)
    favorite_verses: Optional[List[int]] = []


class DevotionalGetAllPayload(BaseModel):
    limit: int
    offset: int
    order_by: str


router = APIRouter()


@router.get("/devotionals", response_model=List[Devotional], summary="Get all devotional for user")
@limiter.limit("50/minute")
async def get_devotionals(request: Request, limit: int = Query(10, ge=1),
                          offset: int = Query(0, ge=0),
                          order_by: str = Query("devotional_date DESC"),
                          current_user: User = Depends(get_current_user_from_cookie)):
    """
    Retrieves all devotional entries for the currently authenticated user.
    Returns a list of devotional entries.
    """
    try:
        devotionals = await get_all_devotionals(user_id=current_user.user_id, limit=limit, offset=offset, order_by=order_by)
    except Exception as e:
        # Log the error e
        # Replace with proper logging
        print(f"Database error fetching devotionals: {e}")
        raise HTTPException(
            status_code=500, detail="Error retrieving devotional data.")

    return devotionals  # FastAPI will serialize this using the Devotional model


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
        devotional = await get_current_devotional(
            user_id=current_user.user_id,  # Assuming your User model has user_id
            devotional_date=today_date
            # Pass db connection/session if required by your CRUD function
            # db=db_session
        )
    except Exception as e:
        # Log the error e
        # Replace with proper logging
        print(f"Database error fetching devotional: {e}")
        raise HTTPException(
            status_code=500, detail="Error retrieving devotional data.")

    if not devotional:
        # It's perfectly normal not to have a devotional for today yet.
        return None

    return devotional  # FastAPI will serialize this using the DevotionalEntry model


# Still use Devotional model for response
@router.post("/devotionals/save", response_model=Devotional)
@limiter.limit("50/minute")
async def save_devotional(request: Request, payload: DevotionalSavePayload, current_user: User = Depends(get_current_user_from_cookie)):
    """
    Saves a devotional entry for the currently authenticated user for today's date.
    Also manages associated favorite verses.

    Returns the full, updated devotional entry including favorite verses.
    """
    today_date: date = date.today()

    try:
        # Step 1: Call the CRUD function to perform the database operations.
        await save_current_devotional(
            user_id=current_user.user_id,
            devotional_date=today_date,
            reflection=payload.reflection,
            favorite_verse_ids=payload.favorite_verses
        )

        # Step 2: Fetch the complete devotional details *after* saving.
        # get_current_devotional returns the structure matching the Devotional model.
        complete_devotional = await get_current_devotional(
            user_id=current_user.user_id,
            devotional_date=today_date
        )

        # Step 3: Handle the unlikely case where fetching right after saving fails.
        if complete_devotional is None:
            log.error(
                f"Failed to retrieve devotional for user {current_user.user_id} on {today_date} immediately after saving.")
            raise HTTPException(
                status_code=500, detail="Failed to retrieve devotional details after saving.")

        # Step 4: Return the complete object fetched in Step 2.
        # This object *will* contain the 'favorite_verses' list if fetched correctly.
        return complete_devotional

    except HTTPException as http_exc:
        # Re-raise known HTTP exceptions
        raise http_exc
    except Exception as e:
        # Log unexpected errors
        log.error(
            f"Error in save_devotional for user {current_user.user_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="An error occurred while saving the devotional data.")
