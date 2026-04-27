package repcheck.ingestion.bills.summary.pipeline

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._

import org.mockito.ArgumentMatchers.{any, anyString, eq => eqTo}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.bills.common.persistence.BillRepository
import repcheck.ingestion.bills.summary.api.BillSummariesApiClient
import repcheck.ingestion.bills.summary.config.BillSummaryConfig
import repcheck.ingestion.bills.summary.persistence.WorkflowRunStepsRepository
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.shared.models.congress.bill.{TextVersionCode, UnrecognizedSummaryVersionCode}
import repcheck.shared.models.congress.dto.bill.{BillReferenceDTO, BillSummaryDTO}

class BillSummaryProcessorSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  /**
   * H2 in-memory transactor for tests that exercise the per-bill `ConnectionIO` flow. The mocked repository operations
   * return pre-staged `ConnectionIO` values, so H2 only needs to be runnable, not schema-compliant.
   */
  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:bill-summary-processor;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private val testConfig = BillSummaryConfig(
    initialLookbackDays = 30,
    watermarkBuffer = 5.minutes,
    congresses = List(118),
    stepName = "bill-summary-pipeline-test",
    httpConcurrency = 1,
  )

  private def makeLogger: PipelineLogger[IO] = {
    val logger = mock[PipelineLogger[IO]]
    when(logger.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(logger.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(logger.debug(any[LogContext], anyString())).thenReturn(IO.unit)
    when(logger.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    logger
  }

  private def makeProcessor(
    apiClient: BillSummariesApiClient[IO] = mock[BillSummariesApiClient[IO]],
    billRepo: BillRepository[ConnectionIO] = mock[BillRepository[ConnectionIO]],
    workflowRepo: WorkflowRunStepsRepository[ConnectionIO] = mock[WorkflowRunStepsRepository[ConnectionIO]],
  ): BillSummaryProcessor[IO] =
    new BillSummaryProcessor[IO](apiClient, billRepo, workflowRepo, testXa, testConfig, makeLogger)

  private def makeSummary(
    congress: Int,
    billType: String,
    number: Long,
    versionCode: Option[String],
  ): BillSummaryDTO =
    BillSummaryDTO(
      actionDate = Some("2024-01-15"),
      actionDesc = Some("test"),
      text = Some("body"),
      updateDate = Some("2024-01-15T00:00:00Z"),
      versionCode = versionCode,
      bill = Some(BillReferenceDTO(congress = congress, billType = billType, number = number)),
    )

  private def makeSummaryNoBill(versionCode: Option[String]): BillSummaryDTO =
    BillSummaryDTO(
      actionDate = None,
      actionDesc = None,
      text = None,
      updateDate = None,
      versionCode = versionCode,
      bill = None,
    )

  // ---------------------------------------------------------------------------
  // pickHighestPerBill — group by natural key, take the entry with the highest
  // progressionOrder per bill. Drops entries without bill ref or with an
  // unmappable / missing versionCode (silent skip — fail-fast happens at the
  // stream-traverse level downstream).
  // ---------------------------------------------------------------------------

  "pickHighestPerBill" should "keep the highest-progressionOrder summary per bill" in {
    val processor = makeProcessor()
    // Same bill, three summaries at different stages: IH (10), RH (30), EAH (60).
    // EAH should win.
    val summaries = List(
      makeSummary(118, "HR", 1, Some("00")), // IH
      makeSummary(118, "HR", 1, Some("13")), // RH
      makeSummary(118, "HR", 1, Some("36")), // EAH
    )

    val result = processor.pickHighestPerBill(summaries)
    val _      = result.size shouldBe 1
    result("118-HR-1")._3 shouldBe TextVersionCode.EAH
  }

  it should "group by natural key — distinct bills do not influence each other's winners" in {
    val processor = makeProcessor()
    val summaries = List(
      makeSummary(118, "HR", 1, Some("00")), // bill A → IH
      makeSummary(118, "HR", 1, Some("36")), // bill A → EAH (winner for A)
      makeSummary(118, "S", 42, Some("70")), // bill B → RTS (winner for B)
    )

    val result = processor.pickHighestPerBill(summaries)
    val _      = result.size shouldBe 2
    val _      = result("118-HR-1")._3 shouldBe TextVersionCode.EAH
    result("118-S-42")._3 shouldBe TextVersionCode.RTS
  }

  it should "drop entries with missing bill reference (bill-scoped endpoint shape)" in {
    val processor = makeProcessor()
    val summaries = List(
      makeSummaryNoBill(Some("00")),
      makeSummary(118, "HR", 1, Some("00")),
    )

    val result = processor.pickHighestPerBill(summaries)
    result.keys shouldBe Set("118-HR-1")
  }

  it should "drop entries with None versionCode (defensive against malformed API)" in {
    val processor = makeProcessor()
    val summaries = List(
      makeSummary(118, "HR", 1, None)
    )

    processor.pickHighestPerBill(summaries) shouldBe empty
  }

  it should "drop entries whose versionCode is not in the SummaryVersionCodeMapper catalog" in {
    // Per the design, a stream-level call site invokes the mapper directly and raises
    // UnrecognizedSummaryVersionCode (Systemic). pickHighestPerBill itself uses the mapper's `.toOption`
    // and silently drops unknown codes; the unmapped rows simply don't contribute to the per-bill
    // winner. End-to-end behavior (halt on unknown) is exercised by streamAll/processOneBill below.
    val processor = makeProcessor()
    val summaries = List(
      makeSummary(118, "HR", 1, Some("99-not-in-catalog")),
      makeSummary(118, "HR", 1, Some("00")),
    )

    val result = processor.pickHighestPerBill(summaries)
    val _      = result.size shouldBe 1
    result("118-HR-1")._3 shouldBe TextVersionCode.IH
  }

  // ---------------------------------------------------------------------------
  // processOneBill — run a placeholder ensure + read existing + conditional
  // write inside a single ConnectionIO transaction. Exercises both branches of
  // the regression guard plus the error path.
  // ---------------------------------------------------------------------------

  private def stubBillRepoFor(
    naturalKey: String,
    findResult: Option[TextVersionCode],
  ): BillRepository[ConnectionIO] = {
    val billRepo = mock[BillRepository[ConnectionIO]]
    when(billRepo.upsertPlaceholder(eqTo(naturalKey))).thenReturn(doobie.free.connection.unit)
    when(billRepo.findExpectedVersion(eqTo(naturalKey))).thenReturn(doobie.free.connection.pure(findResult))
    when(billRepo.updateExpectedVersion(eqTo(naturalKey), any[TextVersionCode]))
      .thenReturn(doobie.free.connection.unit)
    billRepo
  }

  "processOneBill" should "upsert placeholder + write expected when no existing stage" in {
    val billRepo  = stubBillRepoFor("118-HR-1", findResult = None)
    val processor = makeProcessor(billRepo = billRepo)
    val triple = (
      BillReferenceDTO(118, "HR", 1L),
      makeSummary(118, "HR", 1, Some("36")),
      TextVersionCode.EAH,
    )

    val result = processor
      .processOneBill("118-HR-1", triple, LogContext(runId = "0", stepName = "test"))
      .unsafeRunSync()

    val _ = result match {
      case ProcessingResult.Succeeded(entityId, _) => entityId shouldBe "118-HR-1"
      case other                                   => fail(s"expected Succeeded but got $other")
    }
    val _ = verify(billRepo, times(1)).upsertPlaceholder(eqTo("118-HR-1"))
    verify(billRepo, times(1)).updateExpectedVersion(eqTo("118-HR-1"), eqTo(TextVersionCode.EAH))
  }

  it should "skip the write when existing stage is at-or-past the new stage (regression guard)" in {
    val billRepo  = stubBillRepoFor("118-HR-1", findResult = Some(TextVersionCode.PL))
    val processor = makeProcessor(billRepo = billRepo)
    val triple = (
      BillReferenceDTO(118, "HR", 1L),
      makeSummary(118, "HR", 1, Some("00")),
      TextVersionCode.IH,
    )

    val result = processor
      .processOneBill("118-HR-1", triple, LogContext(runId = "0", stepName = "test"))
      .unsafeRunSync()

    val _ = result match {
      case ProcessingResult.Skipped(entityId, reason) =>
        val _ = entityId shouldBe "118-HR-1"
        reason should include("already-at-or-past-stage")
      case other => fail(s"expected Skipped but got $other")
    }
    verify(billRepo, never()).updateExpectedVersion(anyString(), any[TextVersionCode])
  }

  it should "advance when new stage's progressionOrder strictly exceeds the existing stage's" in {
    val billRepo  = stubBillRepoFor("118-HR-1", findResult = Some(TextVersionCode.IH))
    val processor = makeProcessor(billRepo = billRepo)
    val triple = (
      BillReferenceDTO(118, "HR", 1L),
      makeSummary(118, "HR", 1, Some("36")),
      TextVersionCode.EAH,
    )

    val _ = processor
      .processOneBill("118-HR-1", triple, LogContext(runId = "0", stepName = "test"))
      .unsafeRunSync()

    verify(billRepo, times(1)).updateExpectedVersion(eqTo("118-HR-1"), eqTo(TextVersionCode.EAH))
  }

  it should "produce a Failed result with classified errorClass on a write error" in {
    val billRepo = mock[BillRepository[ConnectionIO]]
    when(billRepo.upsertPlaceholder(anyString())).thenReturn(doobie.free.connection.unit)
    when(billRepo.findExpectedVersion(anyString()))
      .thenReturn(doobie.free.connection.raiseError[Option[TextVersionCode]](new java.io.IOException("network blip")))
    val processor = makeProcessor(billRepo = billRepo)
    val triple = (
      BillReferenceDTO(118, "HR", 1L),
      makeSummary(118, "HR", 1, Some("00")),
      TextVersionCode.IH,
    )

    val result = processor
      .processOneBill("118-HR-1", triple, LogContext(runId = "0", stepName = "test"))
      .unsafeRunSync()

    result match {
      case ProcessingResult.Failed(entityId, _, errorClass) =>
        val _ = entityId shouldBe "118-HR-1"
        errorClass shouldBe "Transient"
      case other => fail(s"expected Failed but got $other")
    }
  }

  // ---------------------------------------------------------------------------
  // classifyError — surfaces the Transient / Systemic taxonomy used by the
  // PipelineExecutor's StepRunSummary (counts toward the run-level success).
  // ---------------------------------------------------------------------------

  "classifyError" should "classify network / IO errors as Transient" in {
    val processor = makeProcessor()
    val _         = processor.classifyError(new java.net.SocketTimeoutException("timed out")) shouldBe "Transient"
    val _         = processor.classifyError(new java.net.ConnectException("refused")) shouldBe "Transient"
    val _         = processor.classifyError(new java.io.IOException("disk full")) shouldBe "Transient"
    processor.classifyError(new java.sql.SQLTransientException("conn reset")) shouldBe "Transient"
  }

  it should "classify UnrecognizedSummaryVersionCode as Systemic (fail-fast on unmapped CRS code)" in {
    val processor = makeProcessor()
    processor.classifyError(UnrecognizedSummaryVersionCode("XX")) shouldBe "Systemic"
  }

  it should "default to Systemic for unrecognized errors (halt rather than retry forever)" in {
    val processor = makeProcessor()
    processor.classifyError(new RuntimeException("unknown")) shouldBe "Systemic"
  }

}
