package repcheck.ingestion.votes.errors

/**
 * Top-level failure for the votes-pipeline processor. Raised by [[repcheck.ingestion.votes.pipeline.VoteProcessor]]
 * when a single vote fails to process end-to-end — DTO conversion, member/bill resolution, change detection,
 * persistence, or event emission. Per-vote failures are isolated via `handleErrorWith` in the stream so one bad vote
 * does not halt the run; the caught `VoteProcessingFailed` materializes as a `ProcessingResult.Failed(voteNaturalKey,
 * reason)` in the stream's summary.
 *
 * The unique message anchor `"Failed to process vote "` lets operator log-search pivot from the summary's `errorCounts`
 * map straight to the per-vote warn line. `naturalKey` identifies the specific roll call; `detail` carries the
 * proximate reason; `cause` wraps the underlying exception (API client failure, DB failure, publish failure, etc.) when
 * one exists.
 */
final case class VoteProcessingFailed(
  naturalKey: String,
  detail: String,
  cause: Option[Throwable] = None,
) extends Exception(s"Failed to process vote $naturalKey: $detail") {
  cause.foreach(initCause)
}
