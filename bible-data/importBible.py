import os
import csv
import psycopg2
from psycopg2.extras import execute_values

# PostgreSQL connection details
DB_NAME = "bible_app"
DB_USER = "junpark"
DB_PASSWORD = ""
DB_HOST = "localhost"
DB_PORT = "5432"

# Path to your CSV file
CSV_FILE_PATH = "/Users/junpark/Desktop/Code_Projects/bible-api/bible-data/asv/asv.csv"

def create_tables(conn):
    """Create the necessary tables in PostgreSQL based on the updated schema"""
    cursor = conn.cursor()
    
    # Drop existing tables if they exist
    cursor.execute("""
    DROP TABLE IF EXISTS user_verse_read, plan_day_completion, reading_sessions, 
                     user_reading_plans, plan_sections, reading_plans, users, 
                     verses, chapters, books CASCADE;
    """)
    
    # Create Bible content tables
    cursor.execute("""
    CREATE TABLE books (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        abbreviation VARCHAR(10) NOT NULL,
        testament VARCHAR(10) NOT NULL,
        position INTEGER NOT NULL
    );

    CREATE TABLE chapters (
        id SERIAL PRIMARY KEY,
        book_id INTEGER REFERENCES books(id),
        chapter_number INTEGER NOT NULL,
        UNIQUE(book_id, chapter_number)
    );

    CREATE TABLE verses (
        id SERIAL PRIMARY KEY,
        chapter_id INTEGER REFERENCES chapters(id),
        verse_number INTEGER NOT NULL,
        text TEXT NOT NULL,
        UNIQUE(chapter_id, verse_number)
    );
    
    -- User account and progress tracking
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Reading plans
    CREATE TABLE reading_plans (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        description TEXT,
        duration_days INTEGER NOT NULL,
        is_public BOOLEAN DEFAULT FALSE
    );

    CREATE TABLE plan_sections (
        id SERIAL PRIMARY KEY,
        plan_id INTEGER REFERENCES reading_plans(id) ON DELETE CASCADE,
        day_number INTEGER NOT NULL,
        title VARCHAR(100),
        description TEXT,
        start_verse_id INTEGER REFERENCES verses(id),
        end_verse_id INTEGER REFERENCES verses(id),
        UNIQUE(plan_id, day_number)
    );

    -- User reading progress
    CREATE TABLE user_reading_plans (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
        plan_id INTEGER REFERENCES reading_plans(id) ON DELETE CASCADE,
        start_date DATE NOT NULL,
        completed_date DATE,
        UNIQUE(user_id, plan_id, start_date)
    );

    CREATE TABLE reading_sessions (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
        session_date DATE NOT NULL,
        duration_minutes INTEGER,
        notes TEXT,
        UNIQUE(user_id, session_date)
    );

    CREATE TABLE user_verse_read (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
        verse_id INTEGER REFERENCES verses(id),
        read_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        reading_session_id INTEGER REFERENCES reading_sessions(id) ON DELETE CASCADE,
        UNIQUE(user_id, verse_id)
    );

    CREATE TABLE plan_day_completion (
        id SERIAL PRIMARY KEY,
        user_reading_plan_id INTEGER REFERENCES user_reading_plans(id) ON DELETE CASCADE,
        plan_section_id INTEGER REFERENCES plan_sections(id) ON DELETE CASCADE,
        completed_date DATE NOT NULL,
        UNIQUE(user_reading_plan_id, plan_section_id)
    );
    
    -- Indexes
    CREATE INDEX idx_verses_chapter_id ON verses(chapter_id);
    CREATE INDEX idx_chapters_book_id ON chapters(book_id);
    CREATE INDEX idx_user_verse_read_user_id ON user_verse_read(user_id);
    CREATE INDEX idx_user_verse_read_verse_id ON user_verse_read(verse_id);
    CREATE INDEX idx_reading_sessions_user_date ON reading_sessions(user_id, session_date);

    -- Full-text search index
    CREATE INDEX idx_verses_text_search ON verses USING GIN (to_tsvector('english', text));
    
    -- Create a view for Bible statistics
    CREATE VIEW bible_stats AS
    SELECT 
        COUNT(*) as total_verses,
        COUNT(DISTINCT chapter_id) as total_chapters,
        COUNT(DISTINCT b.id) as total_books
    FROM 
        verses v
    JOIN 
        chapters c ON v.chapter_id = c.id
    JOIN 
        books b ON c.book_id = b.id;
    """)
    
    conn.commit()

def import_from_csv(conn, csv_file_path):
    """Import Bible data from CSV file following the format: Book,Chapter,Verse,Text"""
    try:
        cursor = conn.cursor()
        
        # Dictionary to store book information
        books_dict = {}
        
        # Dictionary to map book names to testament
        testament_map = {
            # Old Testament
            "Genesis": "OT", "Exodus": "OT", "Leviticus": "OT", "Numbers": "OT", 
            "Deuteronomy": "OT", "Joshua": "OT", "Judges": "OT", "Ruth": "OT", 
            "1 Samuel": "OT", "2 Samuel": "OT", "1 Kings": "OT", "2 Kings": "OT", 
            "1 Chronicles": "OT", "2 Chronicles": "OT", "Ezra": "OT", "Nehemiah": "OT", 
            "Esther": "OT", "Job": "OT", "Psalms": "OT", "Proverbs": "OT", 
            "Ecclesiastes": "OT", "Song of Solomon": "OT", "Isaiah": "OT", "Jeremiah": "OT", 
            "Lamentations": "OT", "Ezekiel": "OT", "Daniel": "OT", "Hosea": "OT", 
            "Joel": "OT", "Amos": "OT", "Obadiah": "OT", "Jonah": "OT", 
            "Micah": "OT", "Nahum": "OT", "Habakkuk": "OT", "Zephaniah": "OT", 
            "Haggai": "OT", "Zechariah": "OT", "Malachi": "OT",
            
            # New Testament
            "Matthew": "NT", "Mark": "NT", "Luke": "NT", "John": "NT", 
            "Acts": "NT", "Romans": "NT", "1 Corinthians": "NT", "2 Corinthians": "NT", 
            "Galatians": "NT", "Ephesians": "NT", "Philippians": "NT", "Colossians": "NT", 
            "1 Thessalonians": "NT", "2 Thessalonians": "NT", "1 Timothy": "NT", "2 Timothy": "NT", 
            "Titus": "NT", "Philemon": "NT", "Hebrews": "NT", "James": "NT", 
            "1 Peter": "NT", "2 Peter": "NT", "1 John": "NT", "2 John": "NT", 
            "3 John": "NT", "Jude": "NT", "Revelation": "NT"
        }
        
        # Dictionary for abbreviations
        book_abbr = {
            "Genesis": "Gen", "Exodus": "Exo", "Leviticus": "Lev", "Numbers": "Num",
            "Deuteronomy": "Deu", "Joshua": "Jos", "Judges": "Jdg", "Ruth": "Rut",
            "1 Samuel": "1Sa", "2 Samuel": "2Sa", "1 Kings": "1Ki", "2 Kings": "2Ki",
            "1 Chronicles": "1Ch", "2 Chronicles": "2Ch", "Ezra": "Ezr", "Nehemiah": "Neh",
            "Esther": "Est", "Job": "Job", "Psalms": "Psa", "Proverbs": "Pro",
            "Ecclesiastes": "Ecc", "Song of Solomon": "Sng", "Isaiah": "Isa", "Jeremiah": "Jer",
            "Lamentations": "Lam", "Ezekiel": "Ezk", "Daniel": "Dan", "Hosea": "Hos",
            "Joel": "Joe", "Amos": "Amo", "Obadiah": "Oba", "Jonah": "Jon",
            "Micah": "Mic", "Nahum": "Nah", "Habakkuk": "Hab", "Zephaniah": "Zep",
            "Haggai": "Hag", "Zechariah": "Zec", "Malachi": "Mal", "Matthew": "Mat",
            "Mark": "Mrk", "Luke": "Luk", "John": "Jhn", "Acts": "Act",
            "Romans": "Rom", "1 Corinthians": "1Co", "2 Corinthians": "2Co", "Galatians": "Gal",
            "Ephesians": "Eph", "Philippians": "Php", "Colossians": "Col", "1 Thessalonians": "1Th",
            "2 Thessalonians": "2Th", "1 Timothy": "1Ti", "2 Timothy": "2Ti", "Titus": "Tit",
            "Philemon": "Phm", "Hebrews": "Heb", "James": "Jam", "1 Peter": "1Pe",
            "2 Peter": "2Pe", "1 John": "1Jo", "2 John": "2Jo", "3 John": "3Jo",
            "Jude": "Jud", "Revelation": "Rev"
        }
        
        # Dictionary to store chapter information
        chapters_dict = {}
        
        # Process CSV file line by line
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            print("Processing CSV file...")
            reader = csv.reader(f)
            next(reader)  # Skip header row
            
            # Process in batches
            verses_batch = []
            batch_size = 5000
            
            for row in reader:
                if len(row) < 4:
                    continue  # Skip malformed rows
                
                book_name, chapter_num, verse_num, verse_text = row
                chapter_num = int(chapter_num)
                verse_num = int(verse_num)
                
                # Ensure the book exists in our books dictionary
                if book_name not in books_dict:
                    testament = testament_map.get(book_name, "OT")  # Default to OT if unknown
                    abbr = book_abbr.get(book_name, book_name[:3])  # Default to first 3 chars
                    position = len(books_dict) + 1  # Sequential position
                    
                    cursor.execute(
                        "INSERT INTO books (name, abbreviation, testament, position) VALUES (%s, %s, %s, %s) RETURNING id",
                        (book_name, abbr, testament, position)
                    )
                    book_id = cursor.fetchone()[0]
                    books_dict[book_name] = book_id
                else:
                    book_id = books_dict[book_name]
                
                # Ensure the chapter exists in our chapters dictionary
                chapter_key = f"{book_name}_{chapter_num}"
                if chapter_key not in chapters_dict:
                    cursor.execute(
                        "INSERT INTO chapters (book_id, chapter_number) VALUES (%s, %s) RETURNING id",
                        (book_id, chapter_num)
                    )
                    chapter_id = cursor.fetchone()[0]
                    chapters_dict[chapter_key] = chapter_id
                else:
                    chapter_id = chapters_dict[chapter_key]
                
                # Add verse to batch
                verses_batch.append((chapter_id, verse_num, verse_text))
                
                # Insert batch if it reaches the batch size
                if len(verses_batch) >= batch_size:
                    execute_values(
                        cursor,
                        "INSERT INTO verses (chapter_id, verse_number, text) VALUES %s",
                        verses_batch
                    )
                    verses_batch = []
            
            # Insert any remaining verses
            if verses_batch:
                execute_values(
                    cursor,
                    "INSERT INTO verses (chapter_id, verse_number, text) VALUES %s",
                    verses_batch
                )
        
        conn.commit()
        print(f"Successfully imported Bible data from {csv_file_path}")
        
        # Print some statistics
        cursor.execute("SELECT COUNT(*) FROM books")
        book_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM chapters")
        chapter_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM verses")
        verse_count = cursor.fetchone()[0]
        
        print(f"Imported {book_count} books, {chapter_count} chapters, and {verse_count} verses")
        
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"Error importing from CSV: {e}")
        return False

def main():
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        
        # # Create tables
        # create_tables(conn)
        
        # # Import data from the CSV file
        # if import_from_csv(conn, CSV_FILE_PATH):
        #     print("Bible import completed successfully!")
        # else:
        #     print("Failed to import Bible data.")

        # Create the full-text search function
        create_search_function(conn)
        # Create the function to track reading progress
        create_progress_function(conn)

        print("Functions created successfully!")
            
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def create_search_function(conn):
    """Create the full-text search function in PostgreSQL"""
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE OR REPLACE FUNCTION search_bible(search_query TEXT) 
    RETURNS TABLE (
        book_name VARCHAR(50),
        chapter_number INTEGER,
        verse_number INTEGER,
        verse_text TEXT,
        rank double precision
    ) AS $$
    BEGIN
        RETURN QUERY
        SELECT 
            b.name AS book_name,
            c.chapter_number,
            v.verse_number,
            v.text AS verse_text,
            ts_rank(to_tsvector('english', v.text), to_tsquery('english', search_query))::double precision AS rank
        FROM 
            verses v
        JOIN 
            chapters c ON v.chapter_id = c.id
        JOIN 
            books b ON c.book_id = b.id
        WHERE 
            to_tsvector('english', v.text) @@ to_tsquery('english', search_query)
        ORDER BY 
            rank DESC;
    END;
    $$ LANGUAGE plpgsql;
    """)
    
    conn.commit()

def create_progress_function(conn):
    """Create the function to track reading progress"""
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE OR REPLACE FUNCTION get_user_reading_progress(uid INTEGER) 
    RETURNS TABLE (
        total_verses INTEGER,
        verses_read INTEGER,
        percentage_read NUMERIC(5,2),
        books_completed INTEGER,
        testament_progress JSONB
    ) AS $$
    BEGIN
        RETURN QUERY
        WITH 
        bible_totals AS (
            SELECT * FROM bible_stats
        ),
        user_stats AS (
            SELECT 
                COUNT(DISTINCT uvr.verse_id) as verses_read,
                COUNT(DISTINCT c.id) FILTER (
                    WHERE NOT EXISTS (
                        SELECT 1 FROM verses v2 
                        WHERE v2.chapter_id = c.id 
                        AND NOT EXISTS (
                            SELECT 1 FROM user_verse_read uvr2 
                            WHERE uvr2.user_id = uid AND uvr2.verse_id = v2.id
                        )
                    )
                ) as chapters_completed,
                COUNT(DISTINCT b.id) FILTER (
                    WHERE NOT EXISTS (
                        SELECT 1 FROM verses v2 
                        JOIN chapters c2 ON v2.chapter_id = c2.id
                        WHERE c2.book_id = b.id 
                        AND NOT EXISTS (
                            SELECT 1 FROM user_verse_read uvr2 
                            WHERE uvr2.user_id = uid AND uvr2.verse_id = v2.id
                        )
                    )
                ) as books_completed,
                jsonb_build_object(
                    'OT', (100.0 * COUNT(DISTINCT uvr.verse_id) FILTER (WHERE b.testament = 'OT') / 
                          NULLIF(COUNT(DISTINCT v.id) FILTER (WHERE b.testament = 'OT'), 0)),
                    'NT', (100.0 * COUNT(DISTINCT uvr.verse_id) FILTER (WHERE b.testament = 'NT') / 
                          NULLIF(COUNT(DISTINCT v.id) FILTER (WHERE b.testament = 'NT'), 0))
                ) as testament_progress
            FROM 
                user_verse_read uvr
            JOIN 
                verses v ON uvr.verse_id = v.id
            JOIN 
                chapters c ON v.chapter_id = c.id
            JOIN 
                books b ON c.book_id = b.id
            WHERE 
                uvr.user_id = uid
        )
        SELECT 
            bt.total_verses,
            COALESCE(us.verses_read, 0) as verses_read,
            CASE 
                WHEN bt.total_verses > 0 THEN 
                    ROUND((COALESCE(us.verses_read, 0)::NUMERIC / bt.total_verses) * 100, 2)
                ELSE 0
            END as percentage_read,
            COALESCE(us.books_completed, 0) as books_completed,
            COALESCE(us.testament_progress, '{}'::JSONB) as testament_progress
        FROM 
            bible_totals bt
        LEFT JOIN 
            user_stats us ON true;
    END;
    $$ LANGUAGE plpgsql;
    """)
    
    conn.commit()

if __name__ == "__main__":
    main()