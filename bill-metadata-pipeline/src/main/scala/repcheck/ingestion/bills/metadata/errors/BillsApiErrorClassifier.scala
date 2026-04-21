package repcheck.ingestion.bills.metadata.errors

import repcheck.pipeline.models.errors.{ErrorClass, ErrorClassifier}

/**
 * Classifier for [[repcheck.ingestion.bills.metadata.api.BillsApiClient]] HTTP failures. Treats the standard
 * Congress.gov transient status codes (429, 500, 502, 503, 504) as [[ErrorClass.Transient]] so the retry wrapper
 * retries them; everything else maps to [[ErrorClass.Systemic]] to fail fast.
 *
 * This replaces a direct reference to `CongressGovErrorClassifier` at the raise site: the client now raises a
 * project-declared [[BillsApiHttpError]] instead of the ingestion-common `CongressGovApiException` so that the
 * sbt-exception-uniqueness plugin's project-exceptions-only check sees a locally-declared Throwable.
 */
object BillsApiErrorClassifier extends ErrorClassifier {

  private val transientStatusCodes: Set[Int] = Set(429, 500, 502, 503, 504)

  def classify(error: Throwable): ErrorClass =
    error match {
      case e: BillsApiHttpError if transientStatusCodes.contains(e.statusCode) =>
        ErrorClass.Transient
      case _: BillsApiHttpError =>
        ErrorClass.Systemic
      case _ =>
        ErrorClass.Systemic
    }

}
