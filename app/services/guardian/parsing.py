import json
from typing import Optional


def extract_json(text: str) -> Optional[dict]:
    """
    Best-effort JSON extraction.
    Handles:
    - Full JSON objects embedded in text: ...{...}...
    - JSON fragments missing braces: '"topic":"Lust","verse":"Matthew",...'
    """
    # 1) Try to find a full {...} object.
    start = text.find("{")
    end = text.rfind("}") + 1
    if start != -1 and end > start:
        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            pass

    # 2) Try to wrap a likely JSON fragment.
    frag = text.strip()
    topic_idx = frag.find('"topic"')
    if topic_idx != -1:
        frag = frag[topic_idx:]
    frag = frag.strip().strip("`").strip()

    if frag.startswith("{") and frag.endswith("}"):
        try:
            return json.loads(frag)
        except json.JSONDecodeError:
            return None

    try:
        return json.loads("{" + frag.strip().strip(",") + "}")
    except json.JSONDecodeError:
        return None


def coerce_to_schema(data: dict) -> Optional[dict]:
    """
    Coerce common near-miss model outputs into:
      {"topic": str, "verse": {"book_name": str, "chapter_number": int, "verse_number": int}, "reason": Optional[str]}
    """
    if not isinstance(data, dict):
        return None

    topic = data.get("topic")
    reason = data.get("reason") or data.get("why")
    verse_obj = data.get("verse")

    # Case A: already correct shape.
    if isinstance(verse_obj, dict):
        return data

    # Case B: model outputs verse as string + numbers at top-level:
    # {"topic":"Lust","verse":"Matthew","chapter_number":5,"verse_number":28}
    if isinstance(verse_obj, str) and "chapter_number" in data and "verse_number" in data:
        return {
            "topic": topic,
            "verse": {
                "book_name": verse_obj,
                "chapter_number": data.get("chapter_number"),
                "verse_number": data.get("verse_number"),
            },
            "reason": reason,
        }

    # Case C: model outputs flat keys:
    # {"topic":"Lust","book_name":"Matthew","chapter_number":5,"verse_number":28}
    if "book_name" in data and "chapter_number" in data and "verse_number" in data:
        return {
            "topic": topic,
            "verse": {
                "book_name": data.get("book_name"),
                "chapter_number": data.get("chapter_number"),
                "verse_number": data.get("verse_number"),
            },
            "reason": reason,
        }

    # Case D: model uses alternate book key (seen in flan outputs):
    # {"topic":"Greed & Gambling","text_name":"Luke","chapter_number":12,"verse_number":15}
    alt_book = data.get("text_name") or data.get("book")
    if alt_book and "chapter_number" in data and "verse_number" in data:
        return {
            "topic": topic,
            "verse": {
                "book_name": alt_book,
                "chapter_number": data.get("chapter_number"),
                "verse_number": data.get("verse_number"),
            },
            "reason": reason,
        }

    return None

