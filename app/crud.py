from app.database import db_connection
from datetime import date
from typing import Optional

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

async def get_current_devotional(user_id: str, devotional_date: date):
    """
    Retrieves a single devotional entry for a specific user and date.

    Args:
        user_id: The ID of the user.
        devotional_date: The specific date of the devotional.

    Returns:
        A dictionary representing the devotional record if found, otherwise None.
        Alternatively, returns a DevotionalEntry Pydantic model instance or None.
    """
    async with db_connection() as conn:
        # Ensure all columns needed are selected
        query = """
            SELECT
                devotional_id,
                user_id,
                devotional_date,
                reflection,
                created_at,
                updated_at
            FROM
                devotionals
            WHERE
                user_id = $1 AND devotional_date = $2;
        """

        # Use fetchrow as we expect at most one record due to the UNIQUE constraint
        record = await conn.fetchrow(query, user_id, devotional_date)

        # No need to explicitly close connection, 'async with' handles it.

        if record:
            # Convert the asyncpg Record object to a dictionary
            return dict(record)
            # --- OR ---
            # If you want to return a Pydantic model directly:
            # from app.models.devotional import DevotionalEntry # Make sure it's imported
            # return DevotionalEntry.parse_obj(dict(record))
        else:
            # No devotional found for this user on this date
            return None

async def save_current_devotional(
    user_id: int, # Assuming user_id is INT in the DB. Adjust if UUID/str.
    devotional_date: date,
    reflection: str
): # Returns the saved/updated record as a dict
    """
    Saves (inserts or updates) a devotional entry's reflection for a specific user and date.

    Uses INSERT ... ON CONFLICT ... DO UPDATE to handle uniqueness based on user_id and date.
    Only sets the 'reflection' field. Other optional fields like title, scripture_reference,
    and prayer will remain NULL or their default value unless set by other means.

    Args:
        user_id: The ID of the user.
        devotional_date: The specific date of the devotional.
        reflection: The main devotional text (required).

    Returns:
        A dictionary representing the newly created or updated devotional record,
        including essential fields like ids, date, reflection, and timestamps.

    Raises:
        Exception: Can raise exceptions from the database driver (e.g., connection errors).
                   Specific constraint errors other than the UNIQUE(user_id, date)
                   would also raise exceptions.
    """
    async with db_connection() as conn:
        # UPSERT Query: Insert or Update based on the unique constraint
        # Only inserting/updating the reflection field.
        query = """
            INSERT INTO devotionals (
                user_id, devotional_date, reflection
            )
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, devotional_date)
            DO UPDATE SET
                reflection = EXCLUDED.reflection,
                updated_at = CURRENT_TIMESTAMP -- Explicitly set update time on conflict
            RETURNING
                devotional_id,
                user_id,
                devotional_date,
                reflection,  -- Return the saved reflection
                created_at,
                updated_at; -- Return timestamps
        """

        # Execute the query and fetch the resulting row (inserted or updated)
        saved_record = await conn.fetchrow(
            query,
            user_id,
            devotional_date,
            reflection
        )

        if not saved_record:
             # This case is unlikely with RETURNING on a successful query
             raise Exception("Database did not return the saved devotional record.")

        # Convert the asyncpg Record object to a dictionary
        return dict(saved_record)

async def get_all_devotionals(
    user_id: str,
    limit: int = 50,
    offset: int = 0, # Use for pagination
    order_by: str = "devotional_date DESC" # Use for sorting
):
    """
    Retrieves a paginated list of devotional entries for a specific user.

    Args:
        user_id: The ID of the user whose devotionals to fetch.
        limit: Maximum number of records to return. Defaults to 50.
        offset: Number of records to skip (for pagination). Defaults to 0.
        order_by: Sort order string (e.g., "devotional_date DESC", "created_at ASC").
                  Must be a column from allowed columns and direction (ASC/DESC).
                  Defaults to "devotional_date DESC". Invalid inputs will use the default.

    Returns:
        A list of dictionaries, each representing a devotional record.
        Returns an empty list if no records are found or if offset exceeds records.
    """

    # --- Input Validation for order_by to prevent SQL Injection ---
    ALLOWED_SORT_COLUMNS = {
        "devotional_date",
        "created_at",
        "updated_at"
    }
    ALLOWED_DIRECTIONS = {"ASC", "DESC"}

    # Default sort order
    safe_order_by_clause = "devotional_date DESC"

    # Attempt to parse the provided order_by string
    parts = order_by.strip().split()
    column_candidate = parts[0].lower() # Normalize column name to lowercase
    direction_candidate = "DESC" # Default direction

    if len(parts) > 1:
        direction_candidate = parts[1].upper() # Normalize direction to uppercase

    # Check if the parsed parts are valid
    if column_candidate in ALLOWED_SORT_COLUMNS and direction_candidate in ALLOWED_DIRECTIONS:
        # If valid, construct the safe ORDER BY clause
        safe_order_by_clause = f"{column_candidate} {direction_candidate}"
    else:
        # Log a warning if an invalid sort parameter was provided (optional)
        log.warning(
            f"Invalid order_by parameter: '{order_by}'. "
            f"Falling back to default: '{safe_order_by_clause}'."
        )
        # Keep the default safe_order_by_clause

    # Ensure limit and offset are non-negative
    safe_limit = max(0, limit)
    safe_offset = max(0, offset)
    # --- End Input Validation ---


    async with db_connection() as conn:
        # Use an f-string ONLY for the validated order_by clause.
        # Use parameterized query ($1, $2, $3) for user input values (user_id, limit, offset).
        query = f"""
            SELECT
                devotional_id,
                user_id,
                devotional_date,
                reflection,
                created_at,
                updated_at
                -- Add other fields if needed
            FROM
                devotionals
            WHERE
                user_id = $1
            ORDER BY
                {safe_order_by_clause} -- Safely inserting validated sort order
            LIMIT $2  -- Parameter for limit
            OFFSET $3; -- Parameter for offset
        """

        # Execute the query with parameters
        records = await conn.fetch(
            query,
            user_id,      # $1
            safe_limit,   # $2
            safe_offset   # $3
        )

        # Convert the list of asyncpg Record objects to a list of dictionaries
        return [dict(record) for record in records]