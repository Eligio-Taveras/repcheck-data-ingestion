package repcheck.members.lismapping.client

import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.xml.XmlFeedClient
import repcheck.members.lismapping.config.LisMappingConfig
import repcheck.shared.models.congress.dto.vote.SenatorLookupXmlDTO

/**
 * Fetches and parses the senate.gov `senators_cfm.xml` feed into a stream of [[SenatorLookupXmlDTO]]s filtered to the
 * configured congress lookback window.
 *
 * The full feed is a single HTTP GET returning ~100 rows, but the interface is expressed as a stream for consistency
 * with the rest of the pipeline (and so downstream processors can `parEvalMap` over it identically to paginated
 * sources).
 *
 * Inclusion rule: a senator is emitted if either
 *   - `isCurrent == true`, OR
 *   - at least one `serviceDates` entry has a `congress` value within `[currentCongress - congressLookbackWindow + 1,
 *     currentCongress]` (inclusive on both ends).
 *
 * Senators with no known `congress` values on any service period and `isCurrent == false` are dropped.
 */
class SenatorLookupXmlClient[F[_]: Async](
  xmlFeedClient: XmlFeedClient[F],
  config: LisMappingConfig,
  logger: PipelineLogger[F],
) {

  // Pipeline step identifier — constant per class, kebab-case, action-focused. Matches the convention used by
  // BillTextAvailabilityChecker/BillMetadataProcessor (see their `StepName` vals). The per-run identity comes from
  // the `correlationId` parameter on `fetchMappings`, not from a hardcoded string.
  private val StepName: String = "senator-lookup-xml-fetch"

  def fetchMappings(correlationId: UUID): Stream[F, SenatorLookupXmlDTO] = {
    val logCtx = LogContext(runId = correlationId.toString, stepName = StepName, correlationId = Some(correlationId))

    Stream
      .eval(
        logger.info(logCtx, s"Fetching senator-lookup XML from ${config.senatorXmlUrl}") *>
          xmlFeedClient.fetchXml(config.senatorXmlUrl)
      )
      .flatMap { elem =>
        val parsed   = SenatorXmlParser.parse(elem)
        val filtered = parsed.filter(inLookbackWindow)
        Stream
          .eval(
            logger.info(
              logCtx,
              s"Parsed ${parsed.size.toString} senators, ${filtered.size.toString} within lookback window " +
                s"[${windowStart.toString}, ${config.currentCongress.toString}]",
            )
          )
          .flatMap(_ => Stream.emits(filtered))
      }
  }

  private def windowStart: Int =
    config.currentCongress - config.congressLookbackWindow + 1

  private def inLookbackWindow(dto: SenatorLookupXmlDTO): Boolean = {
    val lowerBound = windowStart
    val upperBound = config.currentCongress
    dto.isCurrent || dto.serviceDates.exists { period =>
      period.congress.exists(c => c >= lowerBound && c <= upperBound)
    }
  }

}
