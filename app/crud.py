from app.database import get_db_connection

# Function to get verses by book and chapter
async def get_verses_by_book_and_chapter(book_name: str, chapter_number: int):
    conn = await get_db_connection()
    
    # SQL query to get the verses based on book and chapter
    query = """
        SELECT v.verse_number, v.text 
        FROM verses v
        JOIN chapters c ON v.chapter_id = c.id
        JOIN books b ON c.book_id = b.id
        WHERE b.name = $1 AND c.chapter_number = $2
        ORDER BY v.verse_number;
    """
    
    # Fetch the verses
    verses = await conn.fetch(query, book_name, chapter_number)
    
    # Close the connection
    await conn.close()
    
    # Convert result into a list of dictionaries
    return [dict(verse) for verse in verses]
