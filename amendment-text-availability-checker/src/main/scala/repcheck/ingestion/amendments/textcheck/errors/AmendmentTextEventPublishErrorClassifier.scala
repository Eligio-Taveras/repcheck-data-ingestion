package repcheck.ingestion.amendments.textcheck.errors

import com.repcheck.utils.errors.{ErrorClass, ErrorClassifier}

/**
 * Classifier for `amendment.text.available` Pub/Sub publish failures. IOException + TimeoutException → Transient; all
 * other Throwables → Systemic. Mirrors the bill-side `EventPublishErrorClassifier` shape — distinct type so retry
 * budgets and observability counters can be split per call site if needed.
 */
object AmendmentTextEventPublishErrorClassifier extends ErrorClassifier {

  override def classify(error: Throwable): ErrorClass =
    error match {
      case _: java.io.IOException                   => ErrorClass.Transient
      case _: java.util.concurrent.TimeoutException => ErrorClass.Transient
      case _                                        => ErrorClass.Systemic
    }

}
