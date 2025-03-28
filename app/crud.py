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
        # Check if the query is a common word or very short
        if len(search_query.strip()) < 4:
            # For common words or short queries, use ILIKE for more inclusive results
            query = """
                SELECT 
                    b.name AS book_name,
                    c.chapter_number,
                    v.verse_number,
                    v.text,
                    1.0 AS rank
                FROM 
                    verses v
                JOIN 
                    chapters c ON v.chapter_id = c.id
                JOIN 
                    books b ON c.book_id = b.id
                WHERE 
                    v.text ILIKE $1
                ORDER BY 
                    b.name, c.chapter_number, v.verse_number
                LIMIT
                    $2;
            """
            
            # Use % for wildcard matching
            like_pattern = f'% {search_query} %'
            results = await conn.fetch(query, like_pattern, limit)
        else:
            # For longer queries, use full-text search
            query = """
                SELECT 
                    b.name AS book_name,
                    c.chapter_number,
                    v.verse_number,
                    v.text,
                    ts_rank(to_tsvector('english', v.text), plainto_tsquery('english', $1))::double precision AS rank
                FROM 
                    verses v
                JOIN 
                    chapters c ON v.chapter_id = c.id
                JOIN 
                    books b ON c.book_id = b.id
                WHERE 
                    to_tsvector('english', v.text) @@ plainto_tsquery('english', $1)
                ORDER BY 
                    rank DESC
                LIMIT
                    $2;
            """
            
            results = await conn.fetch(query, search_query, limit)
        
        # If no results with the first method, try the other method
        if not results:
            if len(search_query.strip()) < 4:
                # Try full-text search as fallback
                query = """
                    SELECT 
                        b.name AS book_name,
                        c.chapter_number,
                        v.verse_number,
                        v.text,
                        ts_rank(to_tsvector('english', v.text), plainto_tsquery('english', $1))::double precision AS rank
                    FROM 
                        verses v
                    JOIN 
                        chapters c ON v.chapter_id = c.id
                    JOIN 
                        books b ON c.book_id = b.id
                    WHERE 
                        to_tsvector('english', v.text) @@ plainto_tsquery('english', $1)
                    ORDER BY 
                        rank DESC
                    LIMIT
                        $2;
                """
                results = await conn.fetch(query, search_query, limit)
            else:
                # Try ILIKE as fallback
                query = """
                    SELECT 
                        b.name AS book_name,
                        c.chapter_number,
                        v.verse_number,
                        v.text,
                        1.0 AS rank
                    FROM 
                        verses v
                    JOIN 
                        chapters c ON v.chapter_id = c.id
                    JOIN 
                        books b ON c.book_id = b.id
                    WHERE 
                        v.text ILIKE $1
                    ORDER BY 
                        b.name, c.chapter_number, v.verse_number
                    LIMIT
                        $2;
                """
                like_pattern = f'%{search_query}%'
                results = await conn.fetch(query, like_pattern, limit)
        
        # Convert result into a list of dictionaries
        return [dict(result) for result in results]
