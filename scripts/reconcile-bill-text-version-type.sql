-- One-shot reconciliation for the bill-metadata UPSERT ownership-boundary fix.
--
-- Before the fix, the bill-metadata UPSERT clobbered bills.text_version_type to NULL on every run (the bill detail
-- API doesn't carry the resolved text version), so ~108K bills that already had text looked "new" to the
-- bill-text-availability-checker and got their text re-emitted + re-downloaded + re-embedded every cycle.
--
-- This repairs text_version_type from the authoritative bill_text_versions row the bill already points at
-- (latest_text_version_id). Idempotent. Run AFTER deploying the upsert fix, otherwise the next metadata run
-- re-clobbers freshly-repaired rows.
--
-- (text_version_type is the only column the availability checker keys on, so it is the only one that needs repair to
--  stop the re-emissions; text_url/text_format/text_date are cosmetic and are dropped entirely in the Phase 1
--  normalization.)

UPDATE bills b
SET text_version_type = v.version_code,
    updated_at = NOW()
FROM bill_text_versions v
WHERE v.id = b.latest_text_version_id
  AND b.latest_text_version_id IS NOT NULL
  AND b.text_version_type IS DISTINCT FROM v.version_code;
