package repcheck.ingestion.bills.textcheck.errors

import repcheck.ingestion.common.errors.HttpStatusErrorClassifier

/**
 * Classifier for [[repcheck.ingestion.bills.textcheck.api.BillTextApiClient]] HTTP failures. Concrete wiring of the
 * shared [[HttpStatusErrorClassifier]]: supplies the Congress.gov transient status set (429/500/502/503/504) and the
 * extractor for the locally-declared [[BillTextApiHttpError]] Throwable.
 *
 * The locally-declared Throwable is necessary — the sbt-exception-uniqueness plugin's project-exceptions-only check
 * scans per-subproject, so dependency-provided Throwables (like ingestion-common's `CongressGovApiException`) register
 * as non-project at the raise site. `BillTextApiHttpError` satisfies that scope while this classifier inherits the
 * actual `classify` logic.
 */
object BillTextApiErrorClassifier extends HttpStatusErrorClassifier(Set(429, 500, 502, 503, 504)) {

  override protected def extractStatusCode(error: Throwable): Option[Int] =
    error match {
      case e: BillTextApiHttpError => Some(e.statusCode)
      case _                       => None
    }

}
