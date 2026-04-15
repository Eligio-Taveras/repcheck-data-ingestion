package repcheck.ingestion.bills.textcheck.errors

import repcheck.pipeline.models.errors.{ErrorClass, ErrorClassifier}

object EventPublishErrorClassifier extends ErrorClassifier {

  def classify(error: Throwable): ErrorClass =
    error match {
      case _: java.io.IOException                   => ErrorClass.Transient
      case _: java.util.concurrent.TimeoutException => ErrorClass.Transient
      case _                                        => ErrorClass.Systemic
    }

}
