import csv
import os
from pathlib import Path
from typing import List, Set, Tuple


def default_asv_csv_path() -> Path:
    # app/services/guardian/books.py -> app/services/guardian -> app/services -> app -> project root
    project_root = Path(__file__).resolve().parents[3]
    return project_root / "bible-data" / "asv" / "asv.csv"


def load_canonical_books(csv_path: str) -> Tuple[List[str], Set[str]]:
    """
    Load unique canonical book names from the ASV CSV (Book,Chapter,Verse,Text).
    Returns (sorted_list, set) for stable prompt + fast membership checks.
    """
    books: Set[str] = set()
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "Book" not in reader.fieldnames:
            raise ValueError("ASV CSV missing 'Book' column header")
        for row in reader:
            book = (row.get("Book") or "").strip()
            if book:
                books.add(book)

    books_list = sorted(books)
    return books_list, books


def get_asv_csv_path() -> str:
    return os.getenv("ASV_CSV_PATH") or str(default_asv_csv_path())

