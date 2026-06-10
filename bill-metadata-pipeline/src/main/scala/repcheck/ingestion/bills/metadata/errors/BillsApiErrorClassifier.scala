package repcheck.ingestion.bills.metadata.errors

import java.io.IOException
import java.net.{SocketException, SocketTimeoutException}
import java.util.concurrent.TimeoutException

import org.http4s.ember.core.EmberException

import com.repcheck.utils.errors.{ErrorClass, ErrorClassifier}

/**
 * Classifier for [[repcheck.ingestion.bills.metadata.api.BillsApiClient]] failures.
 *
 * Treats the following as Transient (retry-eligible):
 *   - HTTP 429 / 500 / 502 / 503 / 504 from Congress.gov (rate limit + upstream errors)
 *   - `org.http4s.ember.core.EmberException.ReachedEndOfStream` — TCP keep-alive race; the connection pool reused a
 *     half-closed socket. Observed at offset=6500 during a 10y backfill where the page-fetch silently aborted with no
 *     retry because EOF was being classified Systemic. Adding it here lets the RetryWrapper's exponential backoff
 *     recover from the brief upstream window where Congress.gov closes its end of the connection.
 *   - Generic transport errors: `SocketException`, `SocketTimeoutException`, `TimeoutException`, `IOException`. The
 *     `withTimeout(30s)` we set on Ember surfaces a hung request as `TimeoutException` — that should retry, not abort.
 *
 * Everything else is Systemic.
 *
 * The classifier walks the `getCause` chain so that wrapped exceptions (e.g. `BillFetchFailed(cause = EmberException)`)
 * are still caught.
 */
object BillsApiErrorClassifier extends ErrorClassifier {

  private val transientStatusCodes: Set[Int] = Set(429, 500, 502, 503, 504)

  override def classify(error: Throwable): ErrorClass = {
    @annotation.tailrec
    def walk(t: Throwable, depth: Int): ErrorClass =
      if (t == null || depth > 10) {
        ErrorClass.Systemic
      } else
        t match {
          case e: BillsApiHttpError if transientStatusCodes.contains(e.statusCode) => ErrorClass.Transient
          case _: EmberException.ReachedEndOfStream                                => ErrorClass.Transient
          case _: SocketTimeoutException                                           => ErrorClass.Transient
          case _: SocketException                                                  => ErrorClass.Transient
          case _: TimeoutException                                                 => ErrorClass.Transient
          case _: IOException                                                      => ErrorClass.Transient
          case _                                                                   => walk(t.getCause, depth + 1)
        }

    walk(error, 0)
  }

}
