from app.database import db_connection

# Function to get verses by book and chapter
async def get_verses_by_book_and_chapter(book_name: str, chapter_number: int):
    async with db_connection() as conn:
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

# Function to search the Bible for specific text
async def search_bible_text(search_query: str, limit: int = 50):
    async with db_connection() as conn:
        # SQL query to search verses using PostgreSQL full-text search
        query = """
            SELECT 
                b.name AS book_name,
                c.chapter_number,
                v.verse_number,
                v.text,
                ts_rank(to_tsvector('english', v.text), to_tsquery('english', $1))::double precision AS rank
            FROM 
                verses v
            JOIN 
                chapters c ON v.chapter_id = c.id
            JOIN 
                books b ON c.book_id = b.id
            WHERE 
                to_tsvector('english', v.text) @@ to_tsquery('english', $1)
            ORDER BY 
                rank DESC
            LIMIT
                $2;
        """
        
        # Fetch the search results
        results = await conn.fetch(query, search_query, limit)
        
        # Close the connection
        await conn.close()
        
        # Convert result into a list of dictionaries
        return [dict(result) for result in results]
