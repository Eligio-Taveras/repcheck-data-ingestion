package repcheck.members.lismapping.client

import cats.effect.Async
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.xml.XmlFeedClient
import repcheck.members.lismapping.config.LisMappingConfig
import repcheck.shared.models.congress.dto.vote.SenatorLookupXmlDTO

/**
 * Fetches and parses the senate.gov `senator-lookup.xml` feed into a stream of [[SenatorLookupXmlDTO]]s filtered to the
 * configured congress lookback window.
 *
 * The feed (`https://www.senate.gov/about/senator-lookup.xml`) contains all senators historically, each with a
 * `<lisid>` (LIS member ID), `<bioguide>` (bioguide ID), `<congresses>` list, and `<service_dates>`. The feed is a
 * single HTTP GET returning ~1900 entries, but the interface is expressed as a stream for consistency with the rest of
 * the pipeline (and so downstream processors can `parEvalMap` over it identically to paginated sources).
 *
 * Inclusion rule: a senator is emitted if either
 *   - `isCurrent == true` (derived from an empty `<end_date>` in their service dates), OR
 *   - at least one `<congress>` value in `<congresses>` falls within `[currentCongress - congressLookbackWindow + 1,
 *     currentCongress]` (inclusive on both ends).
 *
 * Senators with no LIS ID (`<lisid>`), no bioguide ID (`<bioguide>`), or no congress data and `isCurrent == false` are
 * dropped silently.
 */
class SenatorLookupXmlClient[F[_]: Async](
  xmlFeedClient: XmlFeedClient[F],
  config: LisMappingConfig,
  logger: PipelineLogger[F],
) {

  private val StepName: String = "senator-lookup-xml-fetch"

  def fetchMappings(runId: Long): Stream[F, SenatorLookupXmlDTO] = {
    val logCtx = LogContext(runId = runId.toString, stepName = StepName)

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
