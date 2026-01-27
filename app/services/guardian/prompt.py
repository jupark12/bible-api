from typing import List, Optional

VERSE_TOPICS: list[str] = [
    "Time Stewardship",
    "Greed & Gambling",
    "Sexual Purity",
    "Comparison & Envy",
    "Contentious Speech",
]


def build_prompt(*, url: str, canonical_books_list: List[str], retry_hint: Optional[str] = None) -> str:
    # Put the canonical list in a reference-only block to reduce the model's tendency to echo it.
    allowed_books = "\n".join(canonical_books_list)
    hint = f"\nIMPORTANT: {retry_hint}\n" if retry_hint else ""

    topics_block = "\n".join(f"- {t}" for t in VERSE_TOPICS)

    return f"""
You are a JSON generator. Output MUST be STRICT JSON only (no markdown, no prose).

Task:
- Analyze the URL and choose a discouraging Bible verse.
- Choose exactly ONE topic from the list in <VERSE_TOPICS>.
- Provide a short reason (1 sentence) explaining why this verse fits the URL/topic.

REFERENCE_DATA (do NOT repeat this in your output):
<CANONICAL_BOOKS>
{allowed_books}
</CANONICAL_BOOKS>

<VERSE_TOPICS>
{topics_block}
</VERSE_TOPICS>

Rules:
- topic MUST exactly match one line from <VERSE_TOPICS>
- book_name MUST exactly match one line from <CANONICAL_BOOKS>
- chapter_number and verse_number must be integers
- Output ONLY the JSON object matching the schema below
- NEVER output the URL or the text "URL:"

Schema (output exactly this shape):
{{"topic":"Sexual Purity","verse":{{"book_name":"Matthew","chapter_number":5,"verse_number":28}},"reason":"This verse directly warns against lustful intent, which matches the URL's content."}}

<INPUT_URL>{url}</INPUT_URL>
{hint}
JSON:
""".strip()

