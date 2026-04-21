# Reindex Search Rebuild

- Added a first-class `moraine reindex --search-only` command with dry-run preview and explicit `--execute` support.
- Rebuild scope is intentionally limited to derived search tables: `search_documents`, `search_postings`, and `search_conversation_terms`.
- The rebuild path reuses the checked-in ClickHouse migration SQL so search projection stays aligned with the current schema instead of the legacy `bin/backfill-search-index` shell helper.
- Unsupported broader reindex modes are not implemented in this slice; canonical tables and raw source replay are left untouched.
