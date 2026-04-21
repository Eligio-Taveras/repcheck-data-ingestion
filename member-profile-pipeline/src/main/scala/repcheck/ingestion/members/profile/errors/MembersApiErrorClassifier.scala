package repcheck.ingestion.members.profile.errors

import repcheck.ingestion.common.errors.HttpStatusErrorClassifier

/**
 * Classifier for [[repcheck.ingestion.members.profile.api.MembersApiClient]] HTTP failures. Concrete wiring of the
 * shared [[HttpStatusErrorClassifier]]: supplies the Congress.gov transient status set (429/500/502/503/504) and the
 * extractor for the locally-declared [[MembersApiHttpError]] Throwable.
 *
 * The locally-declared Throwable is necessary — the sbt-exception-uniqueness plugin's project-exceptions-only check
 * scans per-subproject, so dependency-provided Throwables (like ingestion-common's `CongressGovApiException`) register
 * as non-project at the raise site. `MembersApiHttpError` satisfies that scope while this classifier inherits the
 * actual `classify` logic.
 */
object MembersApiErrorClassifier extends HttpStatusErrorClassifier(Set(429, 500, 502, 503, 504)) {

  override protected def extractStatusCode(error: Throwable): Option[Int] =
    error match {
      case e: MembersApiHttpError => Some(e.statusCode)
      case _                      => None
    }

}
