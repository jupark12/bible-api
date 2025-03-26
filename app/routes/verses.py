# app/routes/verses.py
from fastapi import APIRouter, HTTPException
from app.crud import get_verses_by_book_and_chapter

# Initialize the router for verses
router = APIRouter()

# Define a GET endpoint to retrieve verses by book and chapter
@router.get("/verses/{book_name}/{chapter_number}")
async def read_verses(book_name: str, chapter_number: int):
    verses = await get_verses_by_book_and_chapter(book_name, chapter_number)
    
    if not verses:
        raise HTTPException(status_code=404, detail="Verses not found")
    
    # Return the list of verses
    return verses