package repcheck.ingestion.votes.pipeline

import java.time.{Instant, LocalDate}
import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie._
import doobie.free.connection

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyLong, anyString}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.votes.errors.VoteProcessingFailed
import repcheck.ingestion.votes.repo.StanceMaterializationStatusRepository
import repcheck.pipeline.models.events.VoteRecordedEvent
import repcheck.shared.models.congress.common.{BillType, Chamber}
import repcheck.shared.models.congress.dos.vote.VoteDO
import repcheck.shared.models.congress.vote.{VoteMethod, VoteType}

/**
 * Unit spec for [[VoteEventEmitter]]. Covers both side effects [[VoteEventEmitter.emitSuccess]] runs in order: (1)
 * `stance_materialization_status` mark — only when the persisted vote is bill-linked — and (2) `VoteRecordedEvent`
 * publish with full event-shape verification. Also pins the publish-failure → typed [[VoteProcessingFailed]] wrapping.
 */
class VoteEventEmitterSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  private val correlationId: UUID = UUID.fromString("11111111-2222-3333-4444-555555555555")

  private val testXa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.h2.Driver",
    url = "jdbc:h2:mem:voteeventemitter;DB_CLOSE_DELAY=-1",
    user = "",
    password = "",
    logHandler = None,
  )

  private def voteDO(
    billId: Option[Long],
    updateDate: Option[Instant] = Some(Instant.parse("2024-06-01T12:00:00Z")),
    voteDate: Option[LocalDate] = Some(LocalDate.parse("2024-05-30")),
    chamber: Chamber = Chamber.House,
    naturalKey: String = "119-House-1-42",
  ): VoteDO =
    VoteDO(
      voteId = 42L,
      naturalKey = naturalKey,
      congress = 119,
      chamber = chamber,
      rollNumber = 42,
      sessionNumber = Some(1),
      billId = billId,
      question = Some("On Passage"),
      voteType = Some(VoteType.Passage),
      voteMethod = Some(VoteMethod.RecordedVote),
      result = Some("Passed"),
      voteDate = voteDate,
      legislationNumber = Some("1234"),
      legislationType = Some(BillType.HR),
      legislationUrl = None,
      sourceDataUrl = None,
      updateDate = updateDate,
      createdAt = None,
      updatedAt = None,
    )

  private def mkLogger: PipelineLogger[IO] = {
    val m = mock[PipelineLogger[IO]]
    when(m.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    m
  }

  private def mkEmitter(
    stanceRepo: StanceMaterializationStatusRepository,
    publisher: IngestionEventPublisher[IO],
  ): VoteEventEmitter[IO] = {
    when(stanceRepo.markHasVotes(anyLong())).thenReturn(connection.pure(()))
    when(publisher.voteRecorded(any[VoteRecordedEvent], any[UUID])).thenReturn(IO.pure("msg-id"))
    new VoteEventEmitter[IO](stanceRepo, publisher, testXa, mkLogger)
  }

  "emitSuccess" should "mark stance + publish event for bill-linked votes" in {
    val stanceRepo = mock[StanceMaterializationStatusRepository]
    val publisher  = mock[IngestionEventPublisher[IO]]
    val emitter    = mkEmitter(stanceRepo, publisher)
    val persisted  = voteDO(billId = Some(200L))

    emitter
      .emitSuccess(persisted, Some("119-HR-1234"), isUpdate = false, correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val _ = verify(stanceRepo, times(1)).markHasVotes(200L)
    verify(publisher, times(1)).voteRecorded(any[VoteRecordedEvent], any[UUID])
  }

  it should "skip stance mark for procedural (non-bill) votes but still publish the event" in {
    val stanceRepo = mock[StanceMaterializationStatusRepository]
    val publisher  = mock[IngestionEventPublisher[IO]]
    val emitter    = mkEmitter(stanceRepo, publisher)
    val persisted  = voteDO(billId = None)

    emitter
      .emitSuccess(persisted, billNaturalKey = None, isUpdate = false, correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val _ = verify(stanceRepo, never()).markHasVotes(anyLong())
    verify(publisher, times(1)).voteRecorded(any[VoteRecordedEvent], any[UUID])
  }

  it should "publish the VoteRecordedEvent with every field populated correctly" in {
    val stanceRepo = mock[StanceMaterializationStatusRepository]
    val publisher  = mock[IngestionEventPublisher[IO]]
    val emitter    = mkEmitter(stanceRepo, publisher)
    val persisted  = voteDO(billId = Some(200L), chamber = Chamber.House)

    emitter
      .emitSuccess(persisted, Some("119-HR-1234"), isUpdate = true, correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val captor = ArgumentCaptor.forClass(classOf[VoteRecordedEvent])
    val _      = verify(publisher, times(1)).voteRecorded(captor.capture(), any[UUID])
    val event  = captor.getValue
    val _      = event.voteNaturalKey shouldBe "119-House-1-42"
    val _      = event.billNaturalKey shouldBe Some("119-HR-1234")
    val _      = event.chamber shouldBe Chamber.House.apiValue
    val _      = event.congress shouldBe 119
    val _      = event.isUpdate shouldBe true
    event.date shouldBe Instant.parse("2024-06-01T12:00:00Z")
  }

  it should "fall back to voteDate at 00:00 UTC when updateDate is missing" in {
    val stanceRepo = mock[StanceMaterializationStatusRepository]
    val publisher  = mock[IngestionEventPublisher[IO]]
    val emitter    = mkEmitter(stanceRepo, publisher)
    val persisted  = voteDO(billId = None, updateDate = None, voteDate = Some(LocalDate.parse("2025-01-25")))

    emitter
      .emitSuccess(persisted, None, isUpdate = false, correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val captor = ArgumentCaptor.forClass(classOf[VoteRecordedEvent])
    val _      = verify(publisher, times(1)).voteRecorded(captor.capture(), any[UUID])
    captor.getValue.date shouldBe Instant.parse("2025-01-25T00:00:00Z")
  }

  it should "fall back to Instant.EPOCH when both updateDate and voteDate are missing" in {
    val stanceRepo = mock[StanceMaterializationStatusRepository]
    val publisher  = mock[IngestionEventPublisher[IO]]
    val emitter    = mkEmitter(stanceRepo, publisher)
    val persisted  = voteDO(billId = None, updateDate = None, voteDate = None)

    emitter
      .emitSuccess(persisted, None, isUpdate = false, correlationId, LogContext("r", "s"))
      .unsafeRunSync()

    val captor = ArgumentCaptor.forClass(classOf[VoteRecordedEvent])
    val _      = verify(publisher, times(1)).voteRecorded(captor.capture(), any[UUID])
    captor.getValue.date shouldBe Instant.EPOCH
  }

  it should "wrap event-publish failures as VoteProcessingFailed with the vote natural key + original cause" in {
    val stanceRepo = mock[StanceMaterializationStatusRepository]
    val publisher  = mock[IngestionEventPublisher[IO]]
    val emitter    = mkEmitter(stanceRepo, publisher)
    when(publisher.voteRecorded(any[VoteRecordedEvent], any[UUID]))
      .thenReturn(IO.raiseError(new RuntimeException("pubsub topic not found")))
    val persisted = voteDO(billId = None, updateDate = None)

    val outcome = emitter
      .emitSuccess(persisted, None, isUpdate = false, correlationId, LogContext("r", "s"))
      .attempt
      .unsafeRunSync()

    outcome match {
      case Left(e: VoteProcessingFailed) =>
        val _ = e.getMessage should include("event publish failed")
        val _ = e.getMessage should include("pubsub topic not found")
        e.getMessage should include(persisted.naturalKey)
      case other => fail(s"expected Left(VoteProcessingFailed), got $other")
    }
  }

}
