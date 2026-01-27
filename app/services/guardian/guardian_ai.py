import logging
import os
from typing import List, Optional, Set

from pydantic import ValidationError

try:
    import torch
    from transformers import AutoConfig, AutoModelForCausalLM, AutoModelForSeq2SeqLM, AutoTokenizer
except ImportError:  # pragma: no cover
    torch = None  # type: ignore[assignment]
    AutoConfig = None  # type: ignore[assignment]
    AutoModelForCausalLM = None  # type: ignore[assignment]
    AutoModelForSeq2SeqLM = None  # type: ignore[assignment]
    AutoTokenizer = None  # type: ignore[assignment]

from app.models import AIRecommendation
from app.services.guardian.books import get_asv_csv_path, load_canonical_books
from app.services.guardian.parsing import coerce_to_schema, extract_json
from app.services.guardian.prompt import build_prompt

logger = logging.getLogger(__name__)


class GuardianAIService:
    _instance: Optional["GuardianAIService"] = None

    def __init__(self) -> None:
        self._tokenizer = None
        self._model = None
        self._is_encoder_decoder: Optional[bool] = None
        self._canonical_books_list: Optional[List[str]] = None
        self._canonical_books_set: Optional[Set[str]] = None

    @classmethod
    def get_instance(cls) -> "GuardianAIService":
        if cls._instance is None:
            cls._instance = GuardianAIService()
        return cls._instance

    def _ensure_canonical_books_loaded(self) -> None:
        if self._canonical_books_list is not None and self._canonical_books_set is not None:
            return
        csv_path = get_asv_csv_path()
        books_list, books_set = load_canonical_books(csv_path)
        self._canonical_books_list = books_list
        self._canonical_books_set = books_set

    def _ensure_model_loaded(self) -> None:
        if self._tokenizer is not None and self._model is not None:
            return

        if (
            AutoTokenizer is None
            or AutoModelForSeq2SeqLM is None
            or AutoModelForCausalLM is None
            or AutoConfig is None
            or torch is None
        ):
            raise RuntimeError("transformers is not installed. Install dependencies from requirements.txt.")

        model_name = "Qwen/Qwen2.5-0.5B-Instruct"

        config = AutoConfig.from_pretrained(model_name)
        self._is_encoder_decoder = bool(getattr(config, "is_encoder_decoder", False))

        self._tokenizer = AutoTokenizer.from_pretrained(model_name)
        if self._is_encoder_decoder:
            self._model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
        else:
            self._model = AutoModelForCausalLM.from_pretrained(model_name)

        self._model.eval()
        logger.warning("guardian_model_loaded model=%s encoder_decoder=%s", model_name, self._is_encoder_decoder)

    def _generate_text(self, prompt: str, max_new_tokens: int = 160) -> str:
        """
        Generate model output as plain text.

        Supports:
        - Encoder/decoder seq2seq models (e.g., Flan-T5)
        - Decoder-only causal/chat models (e.g., Qwen2.5 Instruct)
        """
        assert self._tokenizer is not None and self._model is not None and torch is not None
        assert self._is_encoder_decoder is not None

        if self._is_encoder_decoder:
            inputs = self._tokenizer(prompt, return_tensors="pt", truncation=True)
            with torch.inference_mode():
                output_ids = self._model.generate(
                    **inputs,
                    max_new_tokens=max_new_tokens,
                    do_sample=False,
                    num_beams=1,
                )
            return self._tokenizer.decode(output_ids[0], skip_special_tokens=True).strip()

        messages = [
            {
                "role": "system",
                "content": "You must output STRICT JSON only. Do not output markdown, prose, or any extra keys.",
            },
            {"role": "user", "content": prompt},
        ]
        try:
            chat_text = self._tokenizer.apply_chat_template(  # type: ignore[attr-defined]
                messages,
                tokenize=False,
                add_generation_prompt=True,
            )
        except Exception:
            chat_text = prompt

        inputs = self._tokenizer(chat_text, return_tensors="pt", truncation=True)
        prompt_len = int(inputs["input_ids"].shape[-1])
        with torch.inference_mode():
            output_ids = self._model.generate(
                **inputs,
                max_new_tokens=max_new_tokens,
                do_sample=False,
                num_beams=1,
                pad_token_id=getattr(self._tokenizer, "eos_token_id", None),
            )
        gen_ids = output_ids[0][prompt_len:]
        return self._tokenizer.decode(gen_ids, skip_special_tokens=True).strip()

    def analyze_url_sync(self, url: str) -> Optional[AIRecommendation]:
        """
        Blocking (CPU-bound) call. Run this in a worker thread from async endpoints.
        """
        self._ensure_model_loaded()
        self._ensure_canonical_books_loaded()
        assert self._canonical_books_set is not None
        assert self._canonical_books_list is not None

        prompt = build_prompt(url=url, canonical_books_list=self._canonical_books_list)
        generated = self._generate_text(prompt, max_new_tokens=160)
        logger.warning("guardian_generated url=%s generated=%r", url, generated)

        data = extract_json(generated)
        if data is None:
            logger.warning("guardian_no_json url=%s", url)
            return None

        coerced = coerce_to_schema(data)
        if coerced is None:
            logger.warning("guardian_json_unusable_shape url=%s data=%s", url, data)
            return None

        try:
            rec = AIRecommendation(**coerced)
            logger.warning("guardian_parsed_json url=%s data=%s", url, coerced)
        except ValidationError:
            logger.warning("guardian_json_validation_failed url=%s data=%s", url, coerced)
            return None

        if rec.verse.book_name not in self._canonical_books_set:
            logger.warning("guardian_noncanonical_book url=%s book_name=%s", url, rec.verse.book_name)
            return None

        return rec

