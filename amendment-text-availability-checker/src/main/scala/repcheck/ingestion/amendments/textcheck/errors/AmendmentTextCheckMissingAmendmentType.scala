package repcheck.ingestion.amendments.textcheck.errors

/**
 * Raised by [[repcheck.ingestion.amendments.textcheck.pipeline.AmendmentTextAvailabilityChecker]] when a candidate
 * amendment row has a NULL `amendment_type`. The checker can't build the `/text` URL without it (the path needs `samdt`
 * / `hamdt` / `suamdt`), so the row is failed with this exception so the failure is unambiguous in logs and counters.
 *
 * In practice this should never happen post db-migrations 0.1.34/0.1.35 (the column is `NOT NULL`), but the DO models
 * the field as `Option[AmendmentType]` because pre-migration data could have nulls and the type system can't undo that.
 * This exception covers the residual edge case.
 */
final case class AmendmentTextCheckMissingAmendmentType(naturalKey: String, amendmentId: Long)
    extends Exception(
      s"Amendment $naturalKey (id=${amendmentId.toString}) has NULL amendment_type — cannot build /text URL"
    )
