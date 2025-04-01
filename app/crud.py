from app.database import db_connection
from datetime import date
from typing import Optional, List, Dict, Any

# Function to get verses by book and chapter


async def get_verses_by_book_and_chapter(book_name: str, chapter_number: int):
    async with db_connection() as conn:
        # SQL query to get the verses based on book and chapter
        query = """
            SELECT v.verse_number, v.text, v.id 
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
        devotional_query = """
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
        devotional_record = await conn.fetchrow(devotional_query, user_id, devotional_date)

        if not devotional_record:
            return None

        devotional_data = dict(devotional_record)
        devotional_id = devotional_data['devotional_id']

        favorite_verses_query = """
            SELECT
                v.id as verse_id,
                b.name as book_name,
                c.chapter_number,
                v.verse_number,
                v.text
            FROM
                devotional_favorite_verses dfv
            JOIN
                verses v ON dfv.verse_id = v.id
            JOIN
                chapters c ON v.chapter_id = c.id
            JOIN
                books b ON c.book_id = b.id
            WHERE
                dfv.devotional_id = $1
            ORDER BY
                b.id, c.chapter_number, v.verse_number;
        """
        favorite_verses_records = await conn.fetch(favorite_verses_query, devotional_id)
        devotional_data['favorite_verses'] = [
            dict(verse) for verse in favorite_verses_records]

        return devotional_data


async def save_current_devotional(
    user_id: str,  # Changed to str for consistency, adjust if needed
    devotional_date: date,
    reflection: str,
    # Expecting a list of verse IDs (integers)
    favorite_verse_ids: Optional[List[int]] = None
) -> Dict[str, Any]:  # Returns the main saved/updated devotional record as a dict
    """
    Saves (inserts or updates) a devotional entry and manages its associated favorite verses.

    Uses a transaction to ensure atomicity:
    1. Upserts the devotional entry (reflection, user_id, date).
    2. Updates the associated favorite verses in `devotional_favorite_verses`:
       - Removes verses no longer in the provided list for this devotional.
       - Adds verses from the list that aren't already associated.

    Args:
        user_id: The ID of the user.
        devotional_date: The specific date of the devotional.
        reflection: The main devotional text.
        favorite_verse_ids: A list of integer verse IDs that should be favorited
                            for this specific devotional entry AFTER the save.
                            If None or empty, all existing favorites for this
                            devotional will be removed.

    Returns:
        A dictionary representing the newly created or updated devotional record
        from the `devotionals` table.

    Raises:
        Exception: Database errors or if the devotional record cannot be saved.
    """
    # Ensure favorite_verse_ids is a set for efficient lookups, handle None
    # Use a set to easily manage additions/deletions and ensure uniqueness
    current_favorite_ids = set(
        favorite_verse_ids) if favorite_verse_ids else set()

    async with db_connection() as conn:
        # Start a transaction to ensure all operations succeed or fail together
        async with conn.transaction():
            # === Step 1: Upsert the main devotional record ===
            upsert_devotional_query = """
                INSERT INTO devotionals (
                    user_id, devotional_date, reflection
                )
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, devotional_date)
                DO UPDATE SET
                    reflection = EXCLUDED.reflection,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING
                    devotional_id,
                    user_id,
                    devotional_date,
                    reflection,
                    created_at,
                    updated_at;
            """
            saved_devotional_record = await conn.fetchrow(
                upsert_devotional_query,
                user_id,
                devotional_date,
                reflection
            )

            if not saved_devotional_record:
                # This should ideally not happen with RETURNING on success
                raise Exception(
                    "Failed to save or update the devotional record.")

            devotional_id = saved_devotional_record['devotional_id']

            # === Step 2: Manage favorite verses for this devotional ===

            # --- 2a: Remove favorite verses NOT in the current list ---
            # Delete rows from devotional_favorite_verses for this devotional_id
            # where the verse_id is NOT in the list we just received.
            # Handle the case where current_favorite_ids is empty (delete all).
            if current_favorite_ids:
                # Use array parameter for NOT IN - $2::int[]
                delete_query = """
                    DELETE FROM devotional_favorite_verses
                    WHERE devotional_id = $1 AND verse_id <> ALL ($2::int[]);
                """
                await conn.execute(delete_query, devotional_id, list(current_favorite_ids))
            else:
                # If the input list is empty, remove all associations for this devotional
                delete_all_query = """
                     DELETE FROM devotional_favorite_verses
                     WHERE devotional_id = $1;
                """
                await conn.execute(delete_all_query, devotional_id)

            # --- 2b: Add new favorite verses from the current list ---
            # Attempt to insert all verse IDs provided.
            # ON CONFLICT DO NOTHING handles verses that already exist efficiently.
            if current_favorite_ids:
                insert_query = """
                    INSERT INTO devotional_favorite_verses (devotional_id, verse_id)
                    VALUES ($1, $2)
                    ON CONFLICT (devotional_id, verse_id) DO NOTHING;
                """
                # Prepare data for executemany or loop
                # Using a loop with execute is often clear and sufficient unless performance profiling shows otherwise
                for verse_id in current_favorite_ids:
                    await conn.execute(insert_query, devotional_id, verse_id)
                 # Alternative using executemany (might be slightly faster for very large lists):
                 # data_to_insert = [(devotional_id, verse_id) for verse_id in current_favorite_ids]
                 # await conn.executemany(insert_query, data_to_insert)

            # Transaction commits automatically if no exceptions were raised

        # Return the main devotional data (not the favorite verses list itself)
        return dict(saved_devotional_record)


async def get_all_devotionals(
    user_id: str,
    limit: int = 50,
    offset: int = 0,  # Use for pagination
    order_by: str = "devotional_date DESC"  # Use for sorting
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
    safe_order_by_clause = "devotional_date DESC"
    parts = order_by.strip().split()
    column_candidate = parts[0].lower()
    direction_candidate = "DESC"
    if len(parts) > 1:
        direction_candidate = parts[1].upper()
    if column_candidate in ALLOWED_SORT_COLUMNS and direction_candidate in ALLOWED_DIRECTIONS:
        safe_order_by_clause = f"{column_candidate} {direction_candidate}"
    else:
        log.warning(
            f"Invalid order_by parameter: '{order_by}'. Falling back to default: '{safe_order_by_clause}'.")

    safe_limit = max(0, limit)
    safe_offset = max(0, offset)

    async with db_connection() as conn:
        devotionals_query = f"""
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
                user_id = $1
            ORDER BY
                {safe_order_by_clause}
            LIMIT $2
            OFFSET $3;
        """
        devotional_records = await conn.fetch(
            devotionals_query, user_id, safe_limit, safe_offset
        )

        if not devotional_records:
            return []

        devotional_data_list = [dict(record) for record in devotional_records]
        devotional_ids = [record['devotional_id']
                          for record in devotional_data_list]

        favorite_verses_query = """
            SELECT
                dfv.devotional_id,
                v.id as verse_id,
                b.name as book_name,
                c.chapter_number,
                v.verse_number,
                v.text
            FROM
                devotional_favorite_verses dfv
            JOIN
                verses v ON dfv.verse_id = v.id
            JOIN
                chapters c ON v.chapter_id = c.id
            JOIN
                books b ON c.book_id = b.id
            WHERE
                dfv.devotional_id = ANY($1::int[])
            ORDER BY
                dfv.devotional_id, b.id, c.chapter_number, v.verse_number;
        """
        favorite_verses_records = await conn.fetch(favorite_verses_query, devotional_ids)

        verses_by_devotional_id = {}
        for verse_record in favorite_verses_records:
            dev_id = verse_record['devotional_id']
            if dev_id not in verses_by_devotional_id:
                verses_by_devotional_id[dev_id] = []
            verses_by_devotional_id[dev_id].append(dict(verse_record))

        for devotional_data in devotional_data_list:
            dev_id = devotional_data['devotional_id']
            devotional_data['favorite_verses'] = verses_by_devotional_id.get(dev_id, [
            ])

        return devotional_data_list
