import anyio
from fastapi import APIRouter, Depends, Request

from app import limiter
from app.crud import get_verse_by_ref
from app.models import GuardianRequest, GuardianResponse, VerseReference
from app.services.guardian.guardian_ai import GuardianAIService

router = APIRouter()
def get_ai_service() -> GuardianAIService:
    return GuardianAIService.get_instance()


@router.post("/analyze", response_model=GuardianResponse)
@limiter.limit("30/minute")
async def analyze_url(request: Request, payload: GuardianRequest, ai_service: GuardianAIService = Depends(get_ai_service)):
    # Stateless/privacy: do not log the URL.
    rec = await anyio.to_thread.run_sync(ai_service.analyze_url_sync, payload.url)

    if not rec:
        return GuardianResponse(
            url=payload.url,
            topic_detected=None,
            recommended_verse_ref=None,
            reason=None,
            verse=None,
        )

    verse_ref: VerseReference = rec.verse
    verse = await get_verse_by_ref(
        book_name=verse_ref.book_name,
        chapter_number=verse_ref.chapter_number,
        verse_number=verse_ref.verse_number,
    )

    # If DB doesn't contain it (should be rare if canonical list matches DB), return ref but no verse content.
    return GuardianResponse(
        url=payload.url,
        topic_detected=rec.topic,
        recommended_verse_ref=verse_ref,
        reason=rec.reason,
        verse=verse,
    )

