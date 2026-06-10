package repcheck.ingestion.members.profile.integration

import java.util.UUID

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._
import doobie.postgres.implicits._

import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import repcheck.ingestion.common.events.IngestionEventPublisher
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.ingestion.common.placeholders.{DefaultPlaceholderCreator, DoobieEntityRepository}
import repcheck.ingestion.members.profile.api.MembersApiClient
import repcheck.ingestion.members.profile.config.MemberProfileConfig
import repcheck.ingestion.members.profile.pipeline.MemberProfileProcessor
import repcheck.members.common.MemberInsertSql
import repcheck.members.common.persistence.{
  DoobieMemberHistoryArchiver,
  DoobieMemberPartyHistoryRepository,
  DoobieMemberRepository,
  DoobieMemberTermRepository,
  MemberWriteInstances,
}
import repcheck.members.common.testing.{DockerRequired, TransactorFixture}
import repcheck.pipeline.models.events.{
  BillTextAvailableEvent,
  BillTextIngestedEvent,
  MemberUpdatedEvent,
  VoteRecordedEvent,
}
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.congress.dto.member.{
  MemberDepictionDTO,
  MemberDetailDTO,
  MemberDetailTermDTO,
  MemberListItemDTO,
  MemberTermSummaryDTO,
  PartyHistoryDTO,
}

import com.repcheck.utils.errors.RetryConfig

/**
 * Cross-pipeline placeholder round-trip integration test for the **member** side.
 *
 * Mirrors `BillMetadataPlaceholderRoundTripIntegrationSpec` (PR #106) but for members:
 *
 *   - **Producer**: bill-metadata-pipeline's `MemberResolver.ensureSponsorPlaceholder` (and its cosponsor sibling)
 *     routes through `placeholderCreator.ensureExists[MemberDO](bioguideId, memberEntityRepo)` whenever a sponsor or
 *     cosponsor bioguideId isn't yet in the `members` table. Same wiring as `DefaultPlaceholderCreator +
 *     DoobieEntityRepository[F, MemberDO]` in production (see `BillMetadataPipeline.scala`'s factory).
 *   - **Consumer**: `MemberProfileProcessor.processMember` is the per-member enrichment entry point, called from the
 *     `streamAll` fan-out in member-profile-pipeline. It fetches `/member/{bioguideId}` detail, converts the DTO,
 *     compares against the stored placeholder via `MemberProfileProcessor.isChanged`, and routes to
 *     `persistInTransaction` (archive → upsert → replaceTerms → appendPartyHistory).
 *
 * ## Why this test didn't exist before
 *
 * `PlaceholderFillIntegrationSpec` in this same package covers a related but DIFFERENT shape: it inserts placeholder
 * member rows via raw SQL (`insertPlaceholder`) and "fills" them via raw `insertMember` (also SQL), then asserts on
 * `findPlaceholders`, LIS-mapping FK preservation, and history-archive behavior. The processor itself is never invoked.
 * That coverage is real and useful — but it doesn't pin the actual processor's enrichment behavior against a
 * placeholder created by the production code path.
 *
 * Same gap shape as the bills bug: the placeholder write was tested in isolation; the placeholder enrichment was tested
 * in isolation; the round-trip was missing. This spec closes the loop by:
 *   - Inserting via the EXACT same `placeholderCreator.ensureExists[MemberDO]` chain metadata-pipeline uses.
 *   - Running the EXACT same `MemberProfileProcessor.processMember` member-profile-pipeline runs in production.
 *   - Asserting end-state in the real DB.
 *
 * ## Note on history archive (now consistent with bills after the bug fix)
 *
 * Earlier `DoobieMemberHistoryArchiver.archiveMember` short-circuited placeholders by including `AND update_date IS NOT
 * NULL` in its existence query. That made every placeholder→real transition invisible to the audit log — the *most*
 * interesting moment in a member's lifecycle (the moment we first learn about them via a sponsorship reference and then
 * enrich them) was unrecorded. Companion fix in this PR drops the clause; an audit row is written for every transition,
 * matching `BillHistoryArchiver`'s behavior.
 *
 * The companion schema migration is db-migrations PR <pending> ("migration 035 — member_history.update_date +
 * first_name + last_name nullable"), mirroring migration 034 for bills. Without that schema change the archive INSERT
 * would fail with `null value in column "update_date" violates not-null constraint`. Test 1 below depends on that
 * migration being applied; locally it's been applied as a one-shot ALTER, CI picks it up after db-migrations releases a
 * new runner image and we bump the version in this repo.
 */
class MemberPlaceholderRoundTripIntegrationSpec
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with TransactorFixture {

  import MemberWriteInstances._

  private lazy val memberRepo       = new DoobieMemberRepository
  private lazy val termRepo         = new DoobieMemberTermRepository
  private lazy val partyHistoryRepo = new DoobieMemberPartyHistoryRepository
  private lazy val historyArchiver  = new DoobieMemberHistoryArchiver
  private lazy val placeholder      = new DefaultPlaceholderCreator[IO]
  private lazy val memberEntityRepo = new DoobieEntityRepository[IO, MemberDO](xa, MemberInsertSql.value)

  private def noOpLogger: PipelineLogger[IO] = {
    val m = mock[PipelineLogger[IO]]
    when(m.info(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.warn(any[LogContext], anyString())).thenReturn(IO.unit)
    when(m.error(any[LogContext], anyString(), any[Option[Throwable]])).thenReturn(IO.unit)
    when(m.debug(any[LogContext], anyString())).thenReturn(IO.unit)
    m
  }

  private def noOpEventPublisher: IngestionEventPublisher[IO] = {
    // Member events are published AFTER the persist transaction commits (see
    // MemberProfileProcessor.emitEventIfEligible). For this test we don't assert on the event itself —
    // we assert on the DB state. The mock just needs to return successful IOs so processMember doesn't crash.
    val p = mock[IngestionEventPublisher[IO]]
    when(p.memberUpdated(any[MemberUpdatedEvent], any[UUID])).thenReturn(IO.pure("test-message-id"))
    when(p.billTextAvailable(any[BillTextAvailableEvent], any[UUID])).thenReturn(IO.pure("test-message-id"))
    when(p.billTextIngested(any[BillTextIngestedEvent], any[UUID])).thenReturn(IO.pure("test-message-id"))
    when(p.voteRecorded(any[VoteRecordedEvent], any[UUID])).thenReturn(IO.pure("test-message-id"))
    p
  }

  private def buildProcessor(apiClient: MembersApiClient[IO]): MemberProfileProcessor[IO] =
    new MemberProfileProcessor[IO](
      apiClient = apiClient,
      memberRepo = memberRepo,
      termRepo = termRepo,
      partyHistoryRepo = partyHistoryRepo,
      historyArchiver = historyArchiver,
      eventPublisher = noOpEventPublisher,
      xa = xa,
      config = MemberProfileConfig(
        congresses = List(118),
        parallelism = 1,
        pageDelay = 0.millis,
        eventPublishRetry =
          RetryConfig(maxRetries = 1, initialBackoffMs = 10L, maxBackoffMs = 100L, backoffMultiplier = 2.0),
      ),
      logger = noOpLogger,
    )

  private def makeListItem(bioguideId: String, updateDate: String = "2025-08-01T00:00:00Z"): MemberListItemDTO =
    MemberListItemDTO(
      bioguideId = bioguideId,
      name = Some("Rep. Jane Doe"),
      partyName = Some("Democratic"),
      state = Some("NY"),
      depiction = Some(MemberDepictionDTO(imageUrl = None, attribution = None)),
      terms = Some(List(MemberTermSummaryDTO(chamber = Some("House of Representatives"), startYear = Some(2023)))),
      updateDate = Some(updateDate),
      url = Some(s"https://api.congress.gov/v3/member/$bioguideId"),
    )

  private def makeDetailDTO(bioguideId: String, updateDate: String = "2025-08-01T00:00:00Z"): MemberDetailDTO =
    MemberDetailDTO(
      bioguideId = bioguideId,
      birthYear = Some("1970"),
      firstName = Some("Jane"),
      lastName = Some("Doe"),
      directOrderName = Some("Jane Doe"),
      invertedOrderName = Some("Doe, Jane"),
      honorificName = Some("Rep. Jane Doe"),
      cosponsoredLegislation = None,
      depiction = Some(MemberDepictionDTO(imageUrl = None, attribution = None)),
      leadership = None,
      partyHistory = Some(
        List(PartyHistoryDTO(partyAbbreviation = Some("D"), partyName = Some("Democratic"), startYear = Some(2023)))
      ),
      sponsoredLegislation = None,
      state = Some("New York"),
      terms = Some(
        List(
          MemberDetailTermDTO(
            chamber = Some("House of Representatives"),
            congress = Some(118),
            endYear = Some(2025),
            memberType = Some("Representative"),
            startYear = Some(2023),
            stateCode = Some("NY"),
            stateName = Some("New York"),
            district = Some(5),
          )
        )
      ),
      updateDate = Some(updateDate),
    )

  private def stubApi(bioguideId: String): MembersApiClient[IO] = {
    val client = mock[MembersApiClient[IO]]
    when(client.fetchDetail(anyString())).thenReturn(IO.pure(makeDetailDTO(bioguideId)))
    client
  }

  private def memberHistoryCount(bioguideId: String): Int =
    sql"""SELECT COUNT(*) FROM member_history mh
          JOIN members m ON m.id = mh.member_id
          WHERE m.natural_key = $bioguideId"""
      .query[Int]
      .unique
      .transact(xa)
      .unsafeRunSync()

  // ------------------------------------------------------------------------
  // Test 1: Metadata-style placeholder write → profile-pipeline enrichment
  // ------------------------------------------------------------------------

  "Round-trip: metadata-pipeline member placeholder → profile-pipeline enriches" should
    "succeed, leave a fully-populated members row, and skip the archive (placeholder-not-archived design)" taggedAs DockerRequired in {
      val bioguideId = "A000099"

      // 1. Producer: identical to MemberResolver.ensureSponsorPlaceholder (lines 40, 96 of MemberResolver.scala).
      // Same DefaultPlaceholderCreator + DoobieEntityRepository[MemberDO] wiring as
      // BillMetadataPipeline's factory. The placeholder shape comes from
      // HasPlaceholder[MemberDO].placeholder(naturalKey) in shared-models, which sets every Option detail field
      // to None and `updateDate = None` (the canonical contract; not Instant.now()).
      placeholder.ensureExists[MemberDO](bioguideId, memberEntityRepo).unsafeRunSync()

      // 2. Pre-state — confirm the row matches the canonical placeholder shape.
      val pre = memberRepo.findByBioguideId(bioguideId).transact(xa).unsafeRunSync()
      val _   = pre.isDefined shouldBe true
      val _ = pre.foreach { m =>
        val _ = m.naturalKey shouldBe bioguideId
        val _ = m.firstName shouldBe None
        val _ = m.lastName shouldBe None
        val _ = m.currentParty shouldBe None
        val _ = m.state shouldBe None
        m.updateDate shouldBe None
      }

      // 3. Run the consumer — exact same processMember entry point that streamAll's fan-out invokes.
      val processor     = buildProcessor(stubApi(bioguideId))
      val correlationId = UUID.randomUUID()
      val result        = processor.processMember(makeListItem(bioguideId), correlationId, runId = 0L).unsafeRunSync()

      // 4. Post-state assertions.
      val _ = result.isSucceeded shouldBe true

      val post = memberRepo.findByBioguideId(bioguideId).transact(xa).unsafeRunSync()
      val _    = post.isDefined shouldBe true
      val _ = post.foreach { m =>
        val _ = m.firstName shouldBe Some("Jane")
        val _ = m.lastName shouldBe Some("Doe")
        val _ = m.directOrderName shouldBe Some("Jane Doe")
        // currentParty is derived from partyHistory in the toDO conversion — verifying it's set proves the
        // full DTO→DO→upsert chain ran end-to-end, not just the trivial fields.
        val _ = m.currentParty.isDefined shouldBe true
        m.updateDate.isDefined shouldBe true
      }

      // 5. member_history MUST contain exactly one row capturing the pre-enrichment placeholder shape.
      // The archiver no longer short-circuits placeholders — every state transition gets audited,
      // including the placeholder→real promotion which is the most interesting transition in a
      // member's lifecycle. Mirrors BillHistoryArchiver post-migration-034. The archived row should
      // have ALL profile fields NULL (the placeholder shape) and update_date NULL.
      val _ = memberHistoryCount(bioguideId) shouldBe 1

      val archivedSnapshot = sql"""SELECT mh.first_name, mh.last_name, mh.current_party, mh.update_date
                                  FROM member_history mh
                                  JOIN members m ON m.id = mh.member_id
                                  WHERE m.natural_key = $bioguideId"""
        .query[(Option[String], Option[String], Option[String], Option[java.time.Instant])]
        .unique
        .transact(xa)
        .unsafeRunSync()
      archivedSnapshot shouldBe ((None, None, None, None))
    }

  // ------------------------------------------------------------------------
  // Test 2: Re-enrichment after a real state already exists — the prior real state is archived.
  // This is the same archive trigger that was always working; included for completeness so a
  // regression that makes the archiver skip ALL writes (not just placeholders) surfaces here.
  // ------------------------------------------------------------------------

  "Round-trip: subsequent enrichment of an enriched member" should
    "archive the prior real state alongside the placeholder snapshot (two history rows total)" taggedAs DockerRequired in {
      val bioguideId = "B000099"

      // 1. Seed a placeholder, enrich it once — placeholder transition is archived (Test 1's contract).
      val _          = placeholder.ensureExists[MemberDO](bioguideId, memberEntityRepo).unsafeRunSync()
      val processor1 = buildProcessor(stubApi(bioguideId))
      val _ = processor1
        .processMember(makeListItem(bioguideId, updateDate = "2025-08-01T00:00:00Z"), UUID.randomUUID(), runId = 0L)
        .unsafeRunSync()

      // After first enrichment: 1 archive row (the placeholder snapshot).
      val _ = memberHistoryCount(bioguideId) shouldBe 1

      // 2. Run a SECOND enrichment with a newer updateDate and changed fields. MemberDiffer detects
      // the diff, isChanged returns true, persistInTransaction archives the prior real state +
      // upserts the new one.
      val newApi = mock[MembersApiClient[IO]]
      when(newApi.fetchDetail(anyString())).thenReturn(
        IO.pure(makeDetailDTO(bioguideId, updateDate = "2026-01-15T00:00:00Z"))
          .map(_.copy(firstName = Some("Janet"))) // mutate to force a diff
      )
      val processor2 = buildProcessor(newApi)
      val result2 = processor2
        .processMember(makeListItem(bioguideId, updateDate = "2026-01-15T00:00:00Z"), UUID.randomUUID(), runId = 0L)
        .unsafeRunSync()
      val _ = result2.isSucceeded shouldBe true

      // 3. member_history now has 2 rows total: the placeholder snapshot + the first-real-state snapshot.
      memberHistoryCount(bioguideId) shouldBe 2
    }

}
