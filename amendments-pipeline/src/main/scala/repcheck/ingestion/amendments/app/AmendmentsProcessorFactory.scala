package repcheck.ingestion.amendments.app

import cats.effect.{Async, Temporal}

import repcheck.ingestion.amendments.api.AmendmentsApiClient
import repcheck.ingestion.amendments.observability.AmendmentMetrics
import repcheck.ingestion.amendments.persistence.DoobieAmendmentRepository
import repcheck.ingestion.amendments.pipeline.AmendmentProcessor
import repcheck.ingestion.bills.common.persistence.DoobieBillRepository
import repcheck.ingestion.common.logging.PipelineLogger
import repcheck.ingestion.common.placeholders.{DefaultPlaceholderCreator, DoobieEntityRepository}
import repcheck.members.common.MemberInsertSql
import repcheck.members.common.persistence.{DoobieMemberRepository, MemberWriteInstances}
import repcheck.shared.models.congress.dos.member.MemberDO

/**
 * Factory that assembles the full [[AmendmentProcessor]] dependency graph from a
 * [[AmendmentsPipelineResources.Resources]] bundle plus the top-level [[AmendmentsPipeline.AppConfig]]. Pure — no IO,
 * no Resource acquisition — so tests can exercise the construction path against a resource bundle backed by mocks.
 *
 * Mirrors `VotesProcessorFactory.build`: the IOApp `run` is pure wiring, this factory is the single place where the
 * concrete repositories, placeholder machinery, API client, and processor are stitched together.
 */
private[app] object AmendmentsProcessorFactory {

  def build[F[_]: Async](
    config: AmendmentsPipeline.AppConfig,
    resources: AmendmentsPipelineResources.Resources[F],
    logger: PipelineLogger[F],
    metrics: AmendmentMetrics,
  ): AmendmentProcessor[F] = {
    import MemberWriteInstances._ // brings Write[MemberDO] into scope for DoobieEntityRepository

    val amendmentRepo = new DoobieAmendmentRepository
    val billRepo      = new DoobieBillRepository
    val memberRepo    = new DoobieMemberRepository

    val placeholderCreator = new DefaultPlaceholderCreator[F]
    val memberEntityRepo   = new DoobieEntityRepository[F, MemberDO](resources.xa, MemberInsertSql.value)

    val apiClient = new AmendmentsApiClient[F](
      config = config.congressApi,
      client = resources.httpClient,
      retryWrapper = resources.retryWrapper,
      temporalInstance = Temporal[F],
    )

    new AmendmentProcessor[F](
      apiClient = apiClient,
      amendmentRepository = amendmentRepo,
      placeholderCreator = placeholderCreator,
      memberRepository = memberRepo,
      memberEntityRepo = memberEntityRepo,
      billRepository = billRepo,
      xa = resources.xa,
      config = config.pipeline,
      logger = logger,
      metrics = metrics,
    )
  }

}
