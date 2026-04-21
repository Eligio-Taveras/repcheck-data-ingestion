package repcheck.ingestion.bills.metadata.errors

import repcheck.ingestion.common.errors.HttpStatusErrorClassifier

/**
 * Classifier for [[repcheck.ingestion.bills.metadata.api.BillsApiClient]] HTTP failures. Concrete wiring of the shared
 * [[HttpStatusErrorClassifier]]: supplies the Congress.gov transient status set (429/500/502/503/504) and the extractor
 * for the locally-declared [[BillsApiHttpError]] Throwable.
 *
 * The locally-declared Throwable is necessary — the sbt-exception-uniqueness plugin's project-exceptions-only check
 * scans per-subproject, so dependency-provided Throwables (like ingestion-common's `CongressGovApiException`) register
 * as non-project at the raise site. `BillsApiHttpError` satisfies that scope while this classifier inherits the actual
 * `classify` logic.
 */
object BillsApiErrorClassifier extends HttpStatusErrorClassifier(Set(429, 500, 502, 503, 504)) {

  override protected def extractStatusCode(error: Throwable): Option[Int] =
    error match {
      case e: BillsApiHttpError => Some(e.statusCode)
      case _                    => None
    }

}
