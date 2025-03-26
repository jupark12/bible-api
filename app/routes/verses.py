# app/routes/verses.py
from fastapi import APIRouter, HTTPException, Query, Request
from app.crud import get_verses_by_book_and_chapter, search_bible_text
from typing import List, Optional
from pydantic import BaseModel
from app import limiter

class Verse(BaseModel):
    verse_number: int
    text: str

class SearchResult(BaseModel):
    book_name: str
    chapter_number: int
    verse_number: int
    text: str
    rank: float
    
# Initialize the router for verses
router = APIRouter()

# Define a GET endpoint to retrieve verses by book and chapter
@router.get("/verses/{book_name}/{chapter_number}", response_model=List[Verse])
@limiter.limit("150/minute")
async def read_verses(request: Request, book_name: str, chapter_number: int):
    verses = await get_verses_by_book_and_chapter(book_name, chapter_number)
    
    if not verses:
        raise HTTPException(status_code=404, detail="Verses not found")
    
    # Return the list of verses
    return verses

# Define a GET endpoint to search the Bible
@router.get("/search", response_model=List[SearchResult])
@limiter.limit("50/minute")
async def search_bible(
    request: Request, 
    query: str = Query(..., description="Text to search for in the Bible"),
    limit: Optional[int] = Query(50, description="Maximum number of results to return")
):
    results = await search_bible_text(query, limit)
    
    if not results:
        raise HTTPException(status_code=404, detail="No verses found matching your search")
    
    return results