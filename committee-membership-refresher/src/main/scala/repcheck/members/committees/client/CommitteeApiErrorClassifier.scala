package repcheck.members.committees.client

import repcheck.members.committees.errors.CommitteeApiHttpError
import repcheck.pipeline.models.errors.{ErrorClass, ErrorClassifier}

private[client] object CommitteeApiErrorClassifier extends ErrorClassifier {

  override def classify(error: Throwable): ErrorClass = error match {
    case e: CommitteeApiHttpError if isTransient(e.statusCode) => ErrorClass.Transient
    case _: java.net.ConnectException                          => ErrorClass.Transient
    case _: java.net.SocketTimeoutException                    => ErrorClass.Transient
    case _: org.http4s.InvalidMessageBodyFailure               => ErrorClass.Systemic
    case _                                                     => ErrorClass.Systemic
  }

  private def isTransient(status: Int): Boolean =
    status == 429 || status == 500 || status == 502 || status == 503 || status == 504

}
