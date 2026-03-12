"""
Parser for the silan-nest translation pipeline.

Fetches novel + chapter data from PostgreSQL, converts it to SilanConfig
records, translates via the DataParser pipeline, then writes the translated
rows back into the database (novel_translations / chapter_translations).

If the target language already exists in the DB the old rows are deleted
before the new ones are inserted – all inside a single transaction.
"""

import os
import uuid
import logging
from typing import Any, Dict, List

from translator import DataParser
from translator import VerboseCallback
from providers import GoogleProvider
from configs import SilanConfig
from postgres_client import PostgresClient

logger = logging.getLogger(__name__)

PARSER_NAME = "silan_nest_novel"

# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

_SQL_NOVEL_DEFAULT_TRANSLATION = """
    SELECT id, novel_id, language_code, title, synopsis, slug
    FROM novel_translations
    WHERE novel_id = %(novel_id)s AND is_default = true
    LIMIT 1;
"""

_SQL_CHAPTERS = """
    SELECT id, novel_id, volume_number, chapter_number, chapter_sub_number
    FROM chapters
    WHERE novel_id = %(novel_id)s
    ORDER BY volume_number, chapter_number, chapter_sub_number;
"""

_SQL_CHAPTER_DEFAULT_TRANSLATIONS = """
    SELECT ct.id, ct.chapter_id, ct.language_code, ct.title, ct.content
    FROM chapter_translations ct
    JOIN chapters c ON c.id = ct.chapter_id
    WHERE c.novel_id = %(novel_id)s AND ct.is_default = true
    ORDER BY c.volume_number, c.chapter_number, c.chapter_sub_number;
"""

_SQL_DELETE_NOVEL_TRANSLATIONS = """
    DELETE FROM novel_translations
    WHERE novel_id = %(novel_id)s AND language_code = %(lang)s;
"""

_SQL_DELETE_CHAPTER_TRANSLATIONS = """
    DELETE FROM chapter_translations
    WHERE chapter_id = ANY(%(chapter_ids)s) AND language_code = %(lang)s;
"""

_SQL_INSERT_NOVEL_TRANSLATION = """
    INSERT INTO novel_translations (id, novel_id, language_code, title, synopsis, slug, is_default)
    VALUES (%(id)s, %(novel_id)s, %(language_code)s, %(title)s, %(synopsis)s, %(slug)s, false);
"""

_SQL_INSERT_CHAPTER_TRANSLATION = """
    INSERT INTO chapter_translations (id, chapter_id, language_code, title, content, is_default)
    VALUES (%(id)s, %(chapter_id)s, %(language_code)s, %(title)s, %(content)s, false);
"""


class SilanNest(DataParser):
    """
    Novel/chapter translator that reads source rows from PostgreSQL,
    translates them via the inherited DataParser pipeline, and persists
    the translated rows back into the database.
    """

    ENTITY_NOVEL = "novel"
    ENTITY_CHAPTER = "chapter"

    def __init__(
        self,
        job_id: str,
        novel_id: str,
        target_lang: str,
        db_client: "PostgresClient",
        *,
        source_lang: str = "en",
        output_dir: str = "outputs/novel_jobs",
        translator_provider: type = GoogleProvider,
        enable_code_filter: bool = False,
        max_example_length: int = 1000,
        max_example_per_thread: int = 50,
        large_chunks_threshold: int = 20000,
        max_list_length_per_thread: int = 3,
        average_string_length_in_list: int = 1600,
    ) -> None:
        self.novel_id = novel_id
        self.db_client = db_client

        # DataParser requires a file_path – create a lightweight placeholder
        os.makedirs(output_dir, exist_ok=True)
        self._placeholder_path = os.path.join(output_dir, f".{job_id}.placeholder")
        if not os.path.isfile(self._placeholder_path):
            open(self._placeholder_path, "w").close()

        parser_name = f"translation_job_{job_id}"

        super().__init__(
            file_path=self._placeholder_path,
            output_dir=output_dir,
            parser_name=parser_name,
            target_config=SilanConfig,
            target_fields=["title", "synopsis", "content"],
            do_translate=True,
            translator=translator_provider,
            source_lang=source_lang,
            target_lang=target_lang,
            no_translated_code=enable_code_filter,
            max_example_length=max_example_length,
            max_example_per_thread=max_example_per_thread,
            large_chunks_threshold=large_chunks_threshold,
            max_list_length_per_thread=max_list_length_per_thread,
            average_string_length_in_list=average_string_length_in_list,
            parser_callbacks=[VerboseCallback],
        )

    # ------------------------------------------------------------------
    # DataParser overrides
    # ------------------------------------------------------------------

    def _chunk_text(self, text: str) -> List[str]:
        """Split ``text`` into chunks that fit within ``max_example_length``.

        Chunks are split on newlines where possible to preserve paragraph
        structure.  If a single paragraph exceeds the limit it is hard-split.
        """
        if len(text) <= self.max_example_length:
            return [text]

        chunks: List[str] = []
        current: List[str] = []
        current_len = 0

        for line in text.splitlines(keepends=True):
            if current_len + len(line) > self.max_example_length and current:
                chunks.append("".join(current))
                current = []
                current_len = 0
            # Hard-split a single line that is longer than the limit
            if len(line) > self.max_example_length:
                for i in range(0, len(line), self.max_example_length):
                    chunks.append(line[i:i + self.max_example_length])
            else:
                current.append(line)
                current_len += len(line)

        if current:
            chunks.append("".join(current))

        return chunks

    def read(self) -> None:
        """Fetch the default novel translation and all chapter translations
        from the database and store them in ``self.data_read``."""
        super().read()

        with self.db_client.connection() as conn:
            with conn.cursor() as cur:
                # Novel default translation
                cur.execute(_SQL_NOVEL_DEFAULT_TRANSLATION, {"novel_id": self.novel_id})
                novel_row = cur.fetchone()

                # Chapters (metadata)
                cur.execute(_SQL_CHAPTERS, {"novel_id": self.novel_id})
                chapters = cur.fetchall()

                # Chapter default translations
                cur.execute(_SQL_CHAPTER_DEFAULT_TRANSLATIONS, {"novel_id": self.novel_id})
                chapter_translations = cur.fetchall()

        if not novel_row:
            raise ValueError(f"No default translation found for novel {self.novel_id}")

        # Override source_lang from the actual language_code stored in the DB
        self.source_lang = novel_row["language_code"]

        self.data_read = {
            "novel": novel_row,
            "chapters": chapters,
            "chapter_translations": chapter_translations,
        }
        logger.info(
            "Read novel %s: %d chapters, %d chapter translations",
            self.novel_id,
            len(chapters),
            len(chapter_translations),
        )

    def convert(self) -> None:
        """Map the raw DB rows into a list of ``SilanConfig``-compatible dicts.

        Each dict represents one translatable entity (the novel itself or a
        single chapter).  Long chapter content is split into a list of chunks
        so the DataParser translates every part instead of truncating.
        """
        super().convert()

        converted: List[Dict[str, Any]] = []
        # Maps qas_id -> number of content chunks so persist can rejoin them
        self._content_chunks: Dict[str, int] = {}

        novel = self.data_read["novel"]
        chapter_trans = self.data_read["chapter_translations"]

        # --- Novel record ---
        synopsis_chunks = self._chunk_text(novel.get("synopsis") or "")
        converted.append({
            "qas_id": f"novel_{novel['novel_id']}",
            "entity_type": self.ENTITY_NOVEL,
            "entity_id": str(novel["novel_id"]),
            "source_translation_id": str(novel["id"]),
            "source_lang": self.source_lang,
            "target_lang": self.target_lang,
            "title": novel.get("title") or "",
            # Store as list so DataParser translates every chunk
            "synopsis": synopsis_chunks if len(synopsis_chunks) > 1 else (synopsis_chunks[0] if synopsis_chunks else ""),
            "content": "",  # novels have no content body
        })
        if len(synopsis_chunks) > 1:
            self._content_chunks[f"novel_{novel['novel_id']}_synopsis"] = len(synopsis_chunks)

        # --- Chapter records ---
        for ct in chapter_trans:
            qas_id = f"chapter_{ct['chapter_id']}"
            content_raw = ct.get("content") or ""
            content_chunks = self._chunk_text(content_raw)

            converted.append({
                "qas_id": qas_id,
                "entity_type": self.ENTITY_CHAPTER,
                "entity_id": str(ct["chapter_id"]),
                "source_translation_id": str(ct["id"]),
                "source_lang": self.source_lang,
                "target_lang": self.target_lang,
                "title": ct.get("title") or "",
                "synopsis": "",  # chapters have no synopsis
                # Store as list when chunked so DataParser translates each part
                "content": content_chunks if len(content_chunks) > 1 else content_raw,
            })
            if len(content_chunks) > 1:
                self._content_chunks[qas_id] = len(content_chunks)

        # Validate every record against the target config
        for record in converted:
            self.validate(record.keys())

        self.converted_data = converted
        logger.info(
            "Converted %d records (1 novel + %d chapters), %d records have chunked content",
            len(converted), len(converted) - 1, len(self._content_chunks),
        )

    # ------------------------------------------------------------------
    # Database write-back
    # ------------------------------------------------------------------

    def _make_slug(self, title: str) -> str:
        """Naive slug generator from a translated title."""
        import re
        slug = title.lower().strip()
        slug = re.sub(r"[^\w\s-]", "", slug)
        slug = re.sub(r"[\s_]+", "-", slug)
        slug = re.sub(r"-+", "-", slug).strip("-")
        return slug[:200] if slug else "untitled"

    def _rejoin(self, value: Any) -> str:
        """Merge a translated list-of-chunks back into a single string."""
        if isinstance(value, list):
            return "".join(value)
        return value or ""

    def persist_translations(self) -> None:
        """Write ``self.converted_data_translated`` back into the database.

        * Deletes any existing rows for ``(entity, target_lang)`` first.
        * Inserts the new translated rows.
        * Everything runs inside a single transaction.
        """
        if not self.converted_data_translated:
            raise RuntimeError("No translated data to persist – run the translation pipeline first.")

        novel_rows: List[Dict] = []
        chapter_rows: List[Dict] = []

        for record in self.converted_data_translated:
            if record["entity_type"] == self.ENTITY_NOVEL:
                novel_rows.append(record)
            elif record["entity_type"] == self.ENTITY_CHAPTER:
                chapter_rows.append(record)

        chapter_ids = [r["entity_id"] for r in chapter_rows]

        with self.db_client.connection() as conn:
            with conn.cursor() as cur:
                # --- Delete existing translations for the target language ---
                if novel_rows:
                    cur.execute(_SQL_DELETE_NOVEL_TRANSLATIONS, {
                        "novel_id": self.novel_id,
                        "lang": self.target_lang,
                    })
                    logger.info("Deleted old novel translations for lang=%s", self.target_lang)

                if chapter_ids:
                    cur.execute(_SQL_DELETE_CHAPTER_TRANSLATIONS, {
                        "chapter_ids": chapter_ids,
                        "lang": self.target_lang,
                    })
                    logger.info("Deleted old chapter translations for lang=%s", self.target_lang)

                # --- Insert new translated rows ---
                for nr in novel_rows:
                    synopsis = self._rejoin(nr["synopsis"])
                    cur.execute(_SQL_INSERT_NOVEL_TRANSLATION, {
                        "id": str(uuid.uuid4()),
                        "novel_id": nr["entity_id"],
                        "language_code": self.target_lang,
                        "title": nr["title"],
                        "synopsis": synopsis,
                        "slug": self._make_slug(nr["title"]),
                    })

                for cr in chapter_rows:
                    content = self._rejoin(cr["content"])
                    cur.execute(_SQL_INSERT_CHAPTER_TRANSLATION, {
                        "id": str(uuid.uuid4()),
                        "chapter_id": cr["entity_id"],
                        "language_code": self.target_lang,
                        "title": cr["title"],
                        "content": content,
                    })

        logger.info(
            "Persisted %d novel + %d chapter translations for lang=%s",
            len(novel_rows),
            len(chapter_rows),
            self.target_lang,
        )

    # ------------------------------------------------------------------
    # High-level orchestration
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Full pipeline: read ➜ convert ➜ translate ➜ persist."""
        logger.info("Starting translation job for novel %s → %s", self.novel_id, self.target_lang)

        self.read()
        self.convert()

        # The inherited save is decorated with @no_args_method (descriptor),
        # so it must be accessed without parentheses.
        self.save

        # Write back to database
        self.persist_translations()

        # Clean up placeholder file
        if os.path.isfile(self._placeholder_path):
            os.remove(self._placeholder_path)

        logger.info("Translation job for novel %s completed.", self.novel_id)

