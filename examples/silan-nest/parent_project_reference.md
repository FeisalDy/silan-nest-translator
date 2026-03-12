
---

# Backend Reference: NestJS Novels Module

This document serves as a technical reference for the `silan-nest` backend. It defines the data structures, queue protocols, and API behavior required for the Python translation microservice.

## 1. Database Schema (TypeORM Entities)

The Python worker should interact with these tables. All primary keys are `UUID`.

### A. Core Hierarchy

* **Novel**: The root entity.
* `status`: varchar (e.g., 'completed').


* **Chapter**: Belongs to a Novel.
* Unique constraint: `[novelId, volumeNumber, chapterNumber, chapterSubNumber]`.


* **Author**: Linked to Novels.

### B. Translation Tables (The "Worker" Target)

These tables store the actual text content and are the primary targets for the Python worker.

| Entity | Table Name | Key Columns | Content Columns |
| --- | --- | --- | --- |
| **NovelTranslation** | `novel_translations` | `novel_id`, `language_code` | `title`, `synopsis`, `slug` |
| **ChapterTranslation** | `chapter_translations` | `chapter_id`, `language_code` | `title`, `content` |
| **AuthorTranslation** | `author_translations` | `author_id`, `language_code` | `name`, `biography` |

---

## 2. BullMQ Queue Integration

The Python worker must connect to the same Redis instance and listen to the following queue.

### Queue: `novel-translation-queue`

* **Job Name**: `translate-novel`
* **Payload Format (`NovelTranslationJobPayload`)**:
```json
{
  "novelId": "uuid-string",
  "targetLang": "string (e.g., 'en', 'id')"
}

```


* **Worker Responsibility**:
1. Fetch all `chapters` linked to `novelId`.
2. Generate new entries in `chapter_translations` and `novel_translations` for the `targetLang`.
3. Use a single database transaction to commit all translated rows.



---

## 3. API Endpoints (Controller)

The AI agent can use these endpoints to verify state or trigger actions.

| Method | Endpoint | Description |
| --- | --- | --- |
| `POST` | `/novels/:novelId/translate` | Triggers a new translation job. Payload: `{ "targetLang": "en" }`. |
| `GET` | `/novels/:novelId/translate/status` | Checks BullMQ status (waiting, active, completed, failed). |
| `GET` | `/novels/:novelId/chapters` | Returns paginated chapters (metadata and truncated content). |
| `GET` | `/novels/import/:jobId/status` | Checks status of the initial novel import job. |

---

## 4. Key Logic & Business Rules (NovelsService)

* **Job ID Generation**: Translation jobs are uniquely identified as `translate-${novelId}`.
* **Concurrency**: The system throws a `BadRequestException` if a translation job for a specific novel is already `active` or `waiting`.
* **Language Selection**:
* The backend defaults to `ENGLISH` (`en`) for display.
* If a specific translation is missing, it falls back to the `isDefault` translation or the first available one.


* **Chapter Navigation**: Chapters are sorted by `volumeNumber` → `chapterNumber` → `chapterSubNumber`.

---

## 5. Implementation Notes for Python Worker

1. **Direct DB Access**: The Python worker should bypass the NestJS API for writing and connect directly to the PostgreSQL database using the entity structures defined above.
2. **BullMQ Compatibility**: Ensure the Python library used (e.g., `bullmq-python`) is compatible with BullMQ's Redis schema.
3. **Transaction Safety**: As discussed, the worker should perform `bulk_update_mappings` or `bulk_insert_mappings` inside a `session.begin()` block to ensure atomicity.

---
